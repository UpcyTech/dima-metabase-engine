(ns metabase.metabot.tools.api-call
  "Tools for the experimental, deliberately unguarded `:megabot` profile: a generic
  \"call the Metabase REST API as the current user\" tool, plus two OpenAPI discovery tools.

  - `call_api`             — issue GET/POST/PUT/DELETE/PATCH against the internal `/api` and read the response.
  - `list_api_endpoints`   — a paged, searchable index of every API endpoint (method / path / description).
  - `describe_api_endpoint`— the request/response schema of one endpoint, so a correct `call_api` can be built.

  None of these carry `:scope`/`:capabilities` metadata, so they are neither scope-filtered nor
  scope-checked at call time — matching the other megabot tools.

  Every `call_api` request is dispatched through the *real* production Ring handler
  (`metabase.server.core/make-handler` over `metabase.api-routes.core/routes`, the exact composition
  from `metabase.core.core`), under the already-bound `metabase.api.common/*current-user-id*`. So the
  API layer's own authentication, permission, and sandboxing middleware run unchanged — this profile
  calls the API *as the current user*, it does not bypass anything. Permission failures come back as
  the API's own 4xx response in `:output`, never as a bypass.

  `metabase.api-routes.core` transitively requires this namespace (its `/metabot` route mounts
  `metabase.metabot.api`), so the routes var is resolved lazily via `requiring-resolve` inside the
  memoized delays to avoid a namespace load cycle."
  (:require
   [clojure.core.async :as a]
   [clojure.string :as str]
   [metabase.api.common :as api]
   [metabase.metabot.settings :as metabot.settings]
   [metabase.metabot.tmpl :as te]
   [metabase.server.core :as server]
   [metabase.server.streaming-response :as streaming-response]
   [metabase.util.i18n :as i18n]
   [metabase.util.json :as json]
   [metabase.util.malli :as mu]
   [metabase.util.malli.registry :as mr]
   [ring.util.codec :as codec])
  (:import
   (jakarta.servlet.http HttpServletResponse)
   (java.io ByteArrayInputStream ByteArrayOutputStream InputStream)
   (metabase.server.streaming_response StreamingResponse)))

(set! *warn-on-reflection* true)

;;; ──────────────────────────────────────────────────────────────────
;;; Shared constants / helpers
;;; ──────────────────────────────────────────────────────────────────

(def ^:private open-map
  "A free-form JSON object as the LLM writes it. Deliberately open so the closed-schemas linter and
  runtime instrumentation don't reject arbitrary request bodies / query params."
  [:map {:closed false ::mr/deliberately-open true}])

(def ^:private http-verbs
  "The HTTP methods call_api accepts — the single source for the arg-schema enum and the endpoint filter."
  ["GET" "POST" "PUT" "DELETE" "PATCH"])
(def ^:private method-enum (into [:enum] http-verbs))
(def ^:private http-methods (set http-verbs))

;;; ──────────────────────────────────────────────────────────────────
;;; Memoized production handler + OpenAPI spec (routes resolved lazily)
;;; ──────────────────────────────────────────────────────────────────

#_{:clj-kondo/ignore [:metabase/modules]}
(def ^:private api-handler
  "The real production Ring handler (full middleware onion), built once. This is the exact
  composition `metabase.core.core` builds for the running server, so JSON body/param parsing,
  current-user binding, exception→API-error conversion and streamed-JSON serialization all apply."
  (delay (server/make-handler
          (server/make-routes (requiring-resolve 'metabase.api-routes.core/routes)))))

#_{:clj-kondo/ignore [:metabase/modules]}
(def ^:private full-spec
  "The complete OpenAPI object (same generator that serves `/api/docs/openapi.json`), built once."
  (delay (api/root-open-api-object (requiring-resolve 'metabase.api-routes.core/routes))))

(defn- first-line [s]
  (let [line (-> (str s) (str/split #"\n" 2) first str/trim)]
    (if (> (count line) 140) (str (subs line 0 140) "…") line)))

(def ^:private endpoint-index
  "A compact, sorted index of every endpoint: `[{:method \"GET\" :path \"/api/…\" :description \"…\"} …]`."
  (delay
    (->> (for [[path ops] (:paths @full-spec)
               [m op]     ops
               :let       [method (str/upper-case (name m))]
               :when      (contains? http-methods method)]
           {:method      method
            :path        path
            :description (first-line (:description op))})
         (sort-by (juxt :path :method))
         vec)))

(def ^:private spec-schemas
  "The flat `components/schemas` map, keyed by string schema name (for `$ref` resolution)."
  (delay (into {} (map (fn [[k v]] [(name k) v])) (get-in @full-spec [:components :schemas]))))

;;; ──────────────────────────────────────────────────────────────────
;;; call_api
;;; ──────────────────────────────────────────────────────────────────

(defn- current-auth-context
  "Auth keys for the in-process request, read from the already-bound current-user dynamic vars (the
  same ones the megabot query tools rely on). `enforce-authentication` reads `:metabase-user-id` off
  the request; `do-with-current-user` derives `*current-user*`/permissions from the id but needs the
  superuser/group-manager flags passed explicitly (they default to false otherwise)."
  []
  {:metabase-user-id  api/*current-user-id*
   :is-superuser?     api/*is-superuser?*
   :is-group-manager? api/*is-group-manager?*
   :user-locale       i18n/*user-locale*})

(defn- build-query-string
  "Encode a map of query params into a query string, repeating the key for sequential values."
  [query-params]
  (when (seq query-params)
    (->> query-params
         (mapcat (fn [[k v]]
                   (let [k (name k)]
                     (if (sequential? v)
                       (map #(str (codec/url-encode k) "=" (codec/url-encode (str %))) v)
                       [(str (codec/url-encode k) "=" (codec/url-encode (str v)))]))))
         (str/join "&"))))

(defn- split-path
  "Normalize an API path to `[uri inline-query-string]`. Strips any scheme/host, ensures a leading
  slash and the `/api` prefix, and separates an inline `?query`."
  [path]
  (let [p        (-> (str path) str/trim (str/replace #"^https?://[^/]+" ""))
        p        (if (str/starts-with? p "/") p (str "/" p))
        [uri qs] (str/split p #"\?" 2)
        uri      (if (or (= uri "/api") (str/starts-with? uri "/api/")) uri (str "/api" uri))]
    [uri qs]))

(defn- build-request [method path query-params body]
  (let [[uri inline-qs] (split-path path)
        qs              (when-let [parts (seq (remove str/blank? [inline-qs (build-query-string query-params)]))]
                          (str/join "&" parts))]
    (merge (current-auth-context)
           {:request-method method
            :uri            uri
            :query-string   qs
            :remote-addr    "127.0.0.1"
            :headers        {"content-type" "application/json"}
            :body           (ByteArrayInputStream. (.getBytes ^String (json/encode (or body {})) "UTF-8"))})))

(defn- invoke
  "Call the async 3-arity handler synchronously, returning the Ring response map. Run in a `future`
  so a server-set thread-interrupt flag can't leak into the agent thread."
  [request]
  @(future
     (let [p (promise)]
       (@api-handler request (fn [resp] (deliver p resp)) (fn [e] (deliver p e)))
       (let [r @p]
         (if (instance? Throwable r) (throw r) r)))))

(defn- realize-streaming-response
  "Realize a `StreamingResponse` body to a string (or bytes for binary), capturing a late-set HTTP
  status (query endpoints call `write-error!` → 403 after `respond` has already returned)."
  [^StreamingResponse sr content-type]
  (with-open [os (ByteArrayOutputStream.)]
    (let [f             (.f sr)
          canceled-chan (a/promise-chan)
          status-atom   (atom nil)
          mock-response (reify HttpServletResponse
                          (isCommitted [_] false)
                          (setStatus [_ status] (reset! status-atom status))
                          (setContentType [_ _])
                          (setHeader [_ _ _]))]
      (binding [streaming-response/*response* mock-response]
        (f os canceled-chan))
      {:body   (let [ba (.toByteArray os)]
                 (if (and content-type (re-find #"json|text" content-type))
                   (String. ba "UTF-8")
                   ba))
       :status @status-atom})))

(defn- coerce-body
  "Turn the raw Ring `:body` (InputStream / byte[] / StreamingResponse / other) into a realized
  value, mirroring the test HTTP client's coercion."
  [response]
  (let [body (:body response)
        ct   (get-in response [:headers "Content-Type"])]
    (cond
      (instance? InputStream body)
      (update response :body slurp)

      (bytes? body)
      (update response :body (fn [^bytes b] (String. b "UTF-8")))

      (instance? StreamingResponse body)
      (let [{:keys [body status]} (realize-streaming-response body ct)]
        (cond-> (assoc response :body body)
          status (assoc :status status)))

      :else response)))

(mu/defn ^{:tool-name "call_api"}
  call-api-tool
  "Call the Metabase REST API as the current user and read the response. This is the general way to
  do anything the product can do — save questions, build dashboards, run actions, trigger sync,
  create alerts, manage collections, and so on. `method` is GET/POST/PUT/DELETE/PATCH. `path` is an
  internal API path such as \"/api/collection\" or just \"/collection\" (the /api prefix is added if
  missing). `query_params` is an optional map of query-string parameters — use `limit`/`offset` on
  list endpoints to keep responses small. `body` is an optional JSON object for writes. Permissions
  are enforced exactly as for a normal request: a forbidden call comes back as the API's own 401/403.
  Discover paths and shapes first with list_api_endpoints and describe_api_endpoint. For reading rows
  of query results, prefer run_warehouse_sql / run_warehouse_query."
  [{:keys [method path query_params body]}
   :- [:map {:closed true}
       [:method       method-enum]
       [:path         :string]
       [:query_params {:optional true} open-map]
       [:body         {:optional true} open-map]]]
  (let [m (keyword (str/lower-case method))]
    (cond
      (and (not= m :get) (metabot.settings/megabot-api-read-only?))
      {:output "Refused: call_api is in read-only mode on this instance — only GET is allowed."}

      :else
      (try
        (let [req        (build-request m path query_params body)
              resp       (-> req invoke coerce-body)
              status     (:status resp)
              b          (:body resp)
              body-str   (cond
                           (string? b) b
                           (nil? b)    ""
                           (bytes? b)  (str "[binary response, " (count b) " bytes]")
                           :else       (json/encode b))
              truncated? (> (count body-str) te/default-max-output-chars)
              shown      (if truncated? (subs body-str 0 te/default-max-output-chars) body-str)]
          {:output            (te/lines
                               (str "HTTP " status)
                               shown
                               (when truncated?
                                 "…[body truncated] — narrow the result with query_params like limit/offset."))
           :structured-output {:status status :method method :path (:uri req)}})
        (catch Exception e
          {:output (str "API call error: " (ex-message e))})))))

;;; ──────────────────────────────────────────────────────────────────
;;; list_api_endpoints
;;; ──────────────────────────────────────────────────────────────────

(def ^:private default-page-size 40)
(def ^:private max-page-size 100)

(mu/defn ^{:tool-name "list_api_endpoints"}
  list-api-endpoints-tool
  "List Metabase REST API endpoints so you can discover what call_api can do. Each result is one line:
  METHOD, path, and a one-line description. `search` filters case-insensitively by substring against
  the method+path+description. `method` narrows to one verb. Results are paged: `page` (1-based,
  default 1) and `page_size` (default 40, max 100); the header line states the total match count and
  the current page so you know whether to request the next page or refine `search`."
  [{:keys [search method page page_size]}
   :- [:map {:closed true}
       [:search    {:optional true} :string]
       [:method    {:optional true} method-enum]
       [:page      {:optional true} :int]
       [:page_size {:optional true} :int]]]
  (try
    (let [matches (cond->> @endpoint-index
                    (not (str/blank? method))
                    (filter #(= (:method %) (str/upper-case method)))

                    (not (str/blank? search))
                    (filter (let [q (str/lower-case search)]
                              (fn [{:keys [method path description]}]
                                (str/includes?
                                 (str/lower-case (str method " " path " " description))
                                 q)))))
          matches (vec matches)
          total   (count matches)
          ps      (-> (or page_size default-page-size) (max 1) (min max-page-size))
          pages   (max 1 (long (Math/ceil (/ (double total) ps))))
          pg      (-> (or page 1) (max 1) (min pages))
          start   (* (dec pg) ps)
          end     (min (+ start ps) total)
          shown   (subvec matches (min start total) end)]
      {:output (te/truncate-output
                (te/lines
                 (format "Matched %d endpoint(s). Page %d/%d, showing %d-%d.%s"
                         total pg pages (if (zero? total) 0 (inc start)) end
                         (if (< pg pages)
                           " Request the next `page`, or refine with `search`."
                           ""))
                 ""
                 (map (fn [{:keys [method path description]}]
                        (format "%-6s %s%s" method path
                                (if (str/blank? description) "" (str "  —  " description))))
                      shown)))})
    (catch Exception e
      {:output (str "Error listing endpoints: " (ex-message e))})))

;;; ──────────────────────────────────────────────────────────────────
;;; describe_api_endpoint
;;; ──────────────────────────────────────────────────────────────────

(def ^:private max-described-schemas
  "Bound on how many referenced component schemas describe_api_endpoint will inline."
  60)

(defn- refs-in-json
  "All schema names referenced via `#/components/schemas/<name>` inside a JSON string."
  [^String s]
  (set (map second (re-seq #"#/components/schemas/([^\"]+)" s))))

(defn- resolve-schemas
  "Transitively collect the component schemas reachable from `seed-refs`, bounded by `max-count`."
  [schemas seed-refs max-count]
  (loop [pending (vec seed-refs)
         seen    #{}
         acc     {}]
    (if (or (empty? pending) (>= (count acc) max-count))
      acc
      (let [nm           (first pending)
            rest-pending (subvec pending 1)]
        (if (or (contains? seen nm) (not (contains? schemas nm)))
          (recur rest-pending (conj seen nm) acc)
          (let [schema     (get schemas nm)
                child-refs (refs-in-json (json/encode schema))]
            (recur (into rest-pending (remove seen child-refs))
                   (conj seen nm)
                   (assoc acc nm schema))))))))

(mu/defn ^{:tool-name "describe_api_endpoint"}
  describe-api-endpoint-tool
  "Show the request and response shape of an API endpoint so you can build a correct call_api call.
  `path` is the endpoint path in its templated form as listed by list_api_endpoints, e.g.
  \"/api/collection/{id}\" (\"/api/collection/:id\" is also accepted). `method` optionally narrows to
  one verb; otherwise every verb on the path is shown. Returns the parameters, request body, and
  responses for each operation, plus the referenced component schemas inlined."
  [{:keys [path method]}
   :- [:map {:closed true}
       [:path   :string]
       [:method {:optional true} method-enum]]]
  (try
    (let [paths      (:paths @full-spec)
          [uri _]    (split-path path)
          templated  (str/replace uri #"/:([^/]+)" "/{$1}")
          ops        (or (get paths templated) (get paths uri))]
      (if-not ops
        {:output (te/lines
                  (str "No endpoint found for path " templated ".")
                  "Use list_api_endpoints (with a search term) to find the exact path.")}
        (let [selected (if (str/blank? method)
                         ops
                         (select-keys ops [(keyword (str/lower-case method))]))]
          (if (empty? selected)
            {:output (str "No " method " operation at " templated ". Available: "
                          (str/join ", " (sort (map (comp str/upper-case name) (keys ops)))))}
            (let [named    (into {} (map (fn [[m op]] [(str/upper-case (name m)) op])) selected)
                  ops-json (json/encode named)
                  used     (resolve-schemas @spec-schemas (refs-in-json ops-json) max-described-schemas)]
              {:output (te/truncate-output
                        (te/lines
                         (str "Endpoint: " templated)
                         ""
                         "Operation(s):"
                         ops-json
                         ""
                         (when (seq used) "Referenced schemas:")
                         (when (seq used) (json/encode used))))})))))
    (catch Exception e
      {:output (str "Error describing endpoint: " (ex-message e))})))
