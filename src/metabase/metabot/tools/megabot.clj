(ns metabase.metabot.tools.megabot
  "Tools for the experimental, deliberately unguarded `:megabot` profile.

  Four tools, none carrying `:scope`/`:capabilities` metadata (so they are neither scope-filtered
  nor scope-checked at call time):
  - `run_warehouse_sql`  — raw SQL against a connected warehouse, returning ACTUAL ROWS to the LLM.
  - `run_warehouse_query`— structured MBQL against a warehouse, returning rows.
  - `query_app_db`       — READ-ONLY SQL against Metabase's own application database.
  - `show_result`        — render a query the model already ran inline in the chat, as a table/chart.

  Both warehouse tools run through the query processor under the already-bound
  `metabase.api.common/*current-user-id*`, so the QP's own data/native-permission and sandboxing
  middleware still apply — this profile runs queries as the current user, it does not bypass
  warehouse permissions. `query_app_db` reads the app DB via `metabase.metabot.db`, which runs the
  SQL in a rollback-only transaction; the tool also refuses any statement that is not
  SELECT / WITH / EXPLAIN / SHOW, so a write is both rejected up front and undone if it slips through.
  The row/output caps below are JVM/context safety valves, not access controls.

  Every successful warehouse run registers its raw legacy query in agent memory under a fresh query
  id — the standard `:query-id`/`:query` structured-output contract `extract-queries` in the agent
  loop picks up — so the model can link it (`[text](metabase://query/<id>)`) or render it with
  `show_result`, which emits the `generated_entity` card data part the frontend runs and renders.
  The model only ever sees a row-capped preview; the rendered card re-runs the full query."
  (:require
   [clojure.string :as str]
   [metabase.api.common :as api]
   [metabase.metabot.agent.links :as links]
   [metabase.metabot.agent.memory :as memory]
   [metabase.metabot.agent.streaming :as streaming]
   [metabase.metabot.db :as metabot.db]
   [metabase.metabot.tmpl :as te]
   [metabase.metabot.tools.charts.create :as create-chart-tools]
   [metabase.metabot.tools.shared :as shared]
   [metabase.metabot.tools.shared.instructions :as instructions]
   [metabase.metabot.tools.util :as metabot.tools.u]
   [metabase.query-processor.core :as qp]
   [metabase.util :as u]
   [metabase.util.json :as json]
   [metabase.util.malli :as mu]
   [metabase.util.malli.registry :as mr]))

(set! *warn-on-reflection* true)

(def ^:private mbql-query-schema
  "A free-form MBQL/legacy query map as the LLM writes it. Deliberately open — the query processor
  validates it downstream, not here."
  [:map {:closed                false
         ::mr/deliberately-open true
         :description           "A Metabase MBQL query map, legacy shape: {\"database\": <id>, \"type\": \"query\", \"query\": {…}}."}])

(def ^:private default-row-limit 100)
(def ^:private max-row-limit 5000)

(defn- clamp-limit [row-limit]
  (-> (or row-limit default-row-limit) (max 1) (min max-row-limit)))

(defn- format-rows
  "Render `cols` (column-name strings) and `rows` (seq of value vectors) as a compact markdown table
  in the tool's `:output` string — the only channel the LLM sees."
  [cols rows]
  (let [header (str/join " | " cols)
        sep    (str/join " | " (repeat (count cols) "---"))
        body   (->> rows
                    (map (fn [row] (str/join " | " (map (fn [v] (if (nil? v) "" (str v))) row))))
                    (str/join "\n"))]
    (te/truncate-output (str header "\n" sep "\n" body "\n\n(" (count rows) " rows)"))))

(defn- error-output
  "A tool error `:output` that carries a short recovery hint, so the model can fix the next call rather
  than only seeing the failure. `msg` is the raw error line; `hint` is the concrete next step to try."
  [msg hint]
  {:output (te/lines msg "" (str "To recover: " hint))})

(def ^:private warehouse-recovery-hint
  (str "Verify the numeric database_id with query_app_db (SELECT id, name, engine FROM metabase_database), "
       "and confirm table/column names via metabase_table / metabase_field, before retrying."))

(def ^:private app-db-recovery-hint
  (str "Check names against the app-db schema — only SELECT / WITH / EXPLAIN / SHOW are allowed. "
       "List a table's columns with SELECT id, name, base_type FROM metabase_field WHERE table_id = <id>."))

(defn- register-query!
  "Store `query` in agent memory under `query-id` right away, when a memory atom is bound. The agent
  loop also records it from `:structured-output` after the iteration; writing it now closes the gap
  for a `show_result` issued as a parallel tool call in the same LLM turn."
  [query-id query]
  (when shared/*memory-atom*
    (swap! shared/*memory-atom* memory/set-query query-id query)))

(defn- run-warehouse-query*
  "Run `query` (a legacy MBQL or native query map) through the QP as the current user, capping rows,
  register the raw query under a fresh query id, and format the result rows into `:output`."
  [query row-limit]
  (let [n      (clamp-limit row-limit)
        result (qp/process-query
                (-> query
                    qp/userland-query-with-default-constraints
                    (assoc :constraints {:max-results           n
                                         :max-results-bare-rows n}
                           :info        {:executed-by api/*current-user-id*
                                         :context     :agent})))]
    (if (= (:status result) :completed)
      (let [cols     (mapv :name (get-in result [:data :cols]))
            rows     (get-in result [:data :rows])
            query-id (u/generate-nano-id)]
        (register-query! query-id query)
        {:output            (te/lines
                             (format-rows cols rows)
                             ""
                             (str "Query ID: " query-id)
                             (str "Link it as " (te/link "text" "metabase://query/" query-id)
                                  ", or render the full result for the user with show_result."))
         :structured-output {:query-id  query-id
                             :query     query
                             :database  (:database query)
                             :cols      cols
                             :row-count (count rows)}})
      (error-output (str "Query failed: " (or (:error result) "unknown error")) warehouse-recovery-hint))))

(mu/defn ^{:tool-name "run_warehouse_sql"}
  run-warehouse-sql-tool
  "Run a raw SQL query against a connected warehouse database and READ THE ACTUAL ROWS it returns.
  Unlike other Metabase assistants, you CAN see these results — read them and answer the user
  directly, quoting real values. `database_id` is the numeric Metabase database id (find it with
  query_app_db on the metabase_database table); `sql` runs verbatim against that database. Optional
  `row_limit` caps rows returned (default 100, max 5000). The result comes back with a Query ID you
  can pass to show_result to render the full result for the user."
  [{:keys [database_id sql row_limit]}
   :- [:map {:closed true}
       [:database_id :int]
       [:sql :string]
       [:row_limit {:optional true} :int]]]
  (try
    (run-warehouse-query* {:database database_id :type :native :native {:query sql}} row_limit)
    (catch Exception e
      (error-output (str "Query error: " (ex-message e)) warehouse-recovery-hint))))

(mu/defn ^{:tool-name "run_warehouse_query"}
  run-warehouse-query-tool
  "Run a structured MBQL query against a connected warehouse and READ THE ACTUAL ROWS it returns.
  `query` is a legacy MBQL map, e.g.
  {\"database\": <db id>, \"type\": \"query\", \"query\": {\"source-table\": <table id>,
   \"aggregation\": [[\"count\"]]}} — using the numeric database/table/field ids you look up with
  query_app_db (metabase_table / metabase_field). Optional `row_limit` (default 100, max 5000). The
  result comes back with a Query ID you can pass to show_result to render the full result for the
  user."
  [{:keys [query row_limit]}
   :- [:map {:closed true}
       [:query mbql-query-schema]
       [:row_limit {:optional true} :int]]]
  (try
    (run-warehouse-query* query row_limit)
    (catch Exception e
      (error-output (str "Query error: " (ex-message e)) warehouse-recovery-hint))))

(def ^:private read-only-leading-keywords
  "The only statement kinds `query_app_db` will run — a fast, friendly rejection of obvious writes.
  It is not the last line of defense: `metabase.metabot.db/run-read-only-app-db-sql` runs everything
  in a rollback-only transaction, which catches writes this prefix check can't (a data-modifying CTE
  starting `with`, `explain analyze <dml>`)."
  #{"select" "with" "explain" "show"})

(defn- read-only-sql? [sql]
  (let [first-word (-> sql str/trim (str/split #"\s+" 2) first (or "") str/lower-case)]
    (contains? read-only-leading-keywords first-word)))

(mu/defn ^{:tool-name "query_app_db"}
  query-app-db-tool
  "Run a READ-ONLY SQL query against Metabase's own APPLICATION database — its internal metadata
  store (databases, tables, fields, users, cards, collections, settings), NOT a data warehouse. Use
  it to discover the ids and schema you need before querying a warehouse, e.g.
  `SELECT id, name, engine FROM metabase_database`. Only SELECT / WITH / EXPLAIN / SHOW are allowed;
  writes are refused. Optional `row_limit` (default 100, max 5000)."
  [{:keys [sql row_limit]}
   :- [:map {:closed true}
       [:sql :string]
       [:row_limit {:optional true} :int]]]
  (if-not (read-only-sql? sql)
    {:output "Refused: query_app_db is read-only. Only SELECT / WITH / EXPLAIN / SHOW are allowed."}
    (try
      (let [n       (clamp-limit row_limit)
            rows    (metabot.db/run-read-only-app-db-sql sql)
            limited (vec (take n rows))]
        {:output            (te/truncate-output (json/encode limited))
         :structured-output {:row-count (count limited)}})
      (catch Exception e
        (error-output (str "AppDB error: " (ex-message e)) app-db-recovery-hint)))))

;;; ──────────────────────────────────────────────────────────────────
;;; Show result
;;; ──────────────────────────────────────────────────────────────────

(def ^:private display-enum
  (into [:enum] shared/chart-types))

(defn- show-result-output
  [chart-id display]
  (te/lines
   "<result>"
   (str "Rendered the full result for the user as a " display " visualization.")
   "</result>"
   "<instructions>"
   (instructions/chart-created-instructions chart-id)
   "The user can now see the full result. Do not repeat the table in text; state the takeaway."
   "</instructions>"))

(mu/defn ^{:tool-name "show_result"}
  show-result-tool
  "Render a query you already ran inline in the chat, as a table (default) or a chart, so the user
  sees the full result rather than only your summary. `query_id` is the Query ID returned by
  run_warehouse_sql / run_warehouse_query. `display` is a Metabase visualization type (table, bar,
  line, area, pie, row, scatter, scalar, ...); pick a chart when the shape of the data calls for one,
  e.g. `line` for a time series, `bar` for a breakdown. `title` is a short human-friendly heading
  shown above the result; `description` is an optional one-sentence explanation of what it shows."
  [{:keys [query_id display title description]}
   :- [:map {:closed true}
       [:query_id :string]
       [:display {:optional true} display-enum]
       [:title :string]
       [:description {:optional true} :string]]]
  (try
    (let [chart-type (keyword (or display "table"))
          {:keys [chart-id query]} (create-chart-tools/create-chart
                                    {:query-id      query_id
                                     :chart-type    chart-type
                                     :queries-state (shared/current-queries-state)})
          ;; the closed `::metabot.schema/chart` shape `extract-charts` also writes, so chart links
          ;; resolve within this turn as well as from the persisted state on later turns
          chart      {:chart_id               chart-id
                      :query_id               query_id
                      :queries                [query]
                      :visualization_settings {:chart_type chart-type}}]
      (when shared/*memory-atom*
        (swap! shared/*memory-atom* memory/set-chart chart-id chart))
      {:output            (show-result-output chart-id (name chart-type))
       :structured-output {:result-type :chart
                           :chart-id    chart-id
                           :query-id    query_id
                           :query       query
                           :chart-type  chart-type}
       :data-parts        [(streaming/viz-part {:entity-id   chart-id
                                                :query-id    query_id
                                                :query       (links/->legacy-mbql query)
                                                :display     chart-type
                                                :title       title
                                                :description description})]})
    (catch Exception e
      (if (:agent-error? (ex-data e))
        (metabot.tools.u/handle-agent-error e)
        {:output (str "Failed to show result: " (ex-message e))}))))
