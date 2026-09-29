(ns metabase.dima.native-occurrence
  "Neutral identity authority for one persisted successful Metabot native query occurrence.

  This namespace owns occurrence lookup, authenticated subject correlation, exact persisted
  query identity, exact serialization/fingerprint and runtime identity linkage only.
  It contains no P13 physical certification grammar and no analytical business semantics."
  (:require
   [clojure.string :as str]
   [metabase.api.common :as api]
   [metabase.lib-be.core :as lib-be]
   [metabase.lib.core :as lib]
   [metabase.lib.serialize :as lib.serialize]
   [metabase.metabot.tools :as metabot.tools]
   [metabase.util :as u]
   [metabase.util.json :as json]
   [toucan2.core :as t2])
  (:import
   (java.math BigInteger)
   (java.nio.charset StandardCharsets)
   (java.security MessageDigest)
   (java.util UUID)))

(set! *warn-on-reflection* true)

(def ^:private engine-repository "UpcyTech/dima-metabase-engine")
(def ^:private pinned-upstream-sha "2ba2485c78d7e00a9a25f82c00fc201da71590c4")
(defonce ^:private runtime-instance-id (str (random-uuid)))

(def ^:dynamic *runtime-identity-override*
  "Test-only in-process override. Production callers never bind this."
  nil)

(def ^:dynamic *env-reader*
  "Indirection for deterministic identity tests. Production value is System/getenv."
  #(System/getenv %))

(defn- fail!
  ([code status message]
   (fail! code status message nil))
  ([code status message data]
   (throw (ex-info message
                   (merge {:status-code status
                           :dima/error-code code}
                          data)))))

(defn- nonblank-env [name]
  (some-> (*env-reader* name) str/trim not-empty))

(defn- require-env! [name]
  (or (nonblank-env name)
      (fail! "ENGINE_IDENTITY_INCOMPLETE" 503
             (str "Required engine identity fact is missing: " name)
             {:identity-field name})))

(defn- assert-full-sha! [field value]
  (when-not (re-matches #"[0-9a-f]{40}" value)
    (fail! "ENGINE_IDENTITY_INVALID" 503
           (str field " must be an exact 40-character lowercase git SHA")
           {:identity-field field}))
  value)

(defn- validate-runtime-identity! [identity]
  (doseq [field [:repository :revision_sha :upstream_base_sha :runtime_tag
                 :build_identity :image_identity :runtime_instance_id]]
    (when (str/blank? (some-> (get identity field) str))
      (fail! "ENGINE_IDENTITY_INCOMPLETE" 503
             (str "Required engine identity fact is missing: " (name field))
             {:identity-field (name field)})))
  (assert-full-sha! "revision_sha" (:revision_sha identity))
  (assert-full-sha! "upstream_base_sha" (:upstream_base_sha identity))
  (when-not (= (:repository identity) engine-repository)
    (fail! "ENGINE_IDENTITY_INVALID" 503
           "Engine repository identity does not match the certified Dima engine repository"))
  (when-not (= (:upstream_base_sha identity) pinned-upstream-sha)
    (fail! "ENGINE_IDENTITY_INVALID" 503
           "Engine upstream base does not match the pinned P13B upstream"))
  (try
    (UUID/fromString (str (:runtime_instance_id identity)))
    (catch IllegalArgumentException _
      (fail! "ENGINE_IDENTITY_INVALID" 503
             "runtime_instance_id must be a UUID"
             {:identity-field "runtime_instance_id"})))
  identity)

(defn runtime-identity
  "Return the request-relevant Dima engine identity. Missing build/deployment facts fail closed."
  []
  (validate-runtime-identity!
   (or *runtime-identity-override*
       {:repository          (or (nonblank-env "DIMA_ENGINE_REPOSITORY") engine-repository)
        :revision_sha        (require-env! "DIMA_ENGINE_REVISION_SHA")
        :upstream_base_sha   (require-env! "DIMA_ENGINE_UPSTREAM_BASE_SHA")
        :runtime_tag         (require-env! "DIMA_ENGINE_RUNTIME_TAG")
        :build_identity      (require-env! "DIMA_ENGINE_BUILD_IDENTITY")
        :image_identity      (require-env! "DIMA_ENGINE_IMAGE_IDENTITY")
        :runtime_instance_id runtime-instance-id})))

(defn- json-wire-value [value]
  ;; Round-trip through Metabase's own JSON encoder so keywords and other wire values
  ;; have exactly the representation the REST client receives.
  (json/decode (json/encode value)))

(defn- canonical-json-value [value]
  (cond
    (map? value)
    (into (sorted-map)
          (map (fn [[k v]]
                 [(str k) (canonical-json-value v)]))
          value)

    (vector? value)
    (mapv canonical-json-value value)

    (sequential? value)
    (mapv canonical-json-value value)

    :else value))

(defn canonical-json
  "Frozen compact JSON used by both engine and Platform fingerprint fixtures."
  [value]
  (json/encode (canonical-json-value (json-wire-value value))))

(defn sha256-hex [^String value]
  (let [digest (.digest (MessageDigest/getInstance "SHA-256")
                        (.getBytes value StandardCharsets/UTF_8))]
    (format "%064x" (BigInteger. 1 digest))))

(defn- restore-persisted-query [query]
  ;; Metabot conversation/message state is stored as JSON. Re-enter through Metabase Lib's
  ;; query constructor with the application-DB metadata provider so Lib owns normalization,
  ;; field typing, and metadata attachment. This is intentionally not a Dima query parser.
  (let [database-id (or (:database query) (get query "database"))]
    (when-not (pos-int? database-id)
      (fail! "NATIVE_QUERY_PRODUCER_INVALID" 409
             "Persisted native query has no positive database id"))
    (lib/query (lib-be/application-database-metadata-provider database-id) query)))

(defn exact-serialized-query
  "Metabase REST/app-DB serialization boundary for one exact internal pMBQL query."
  [query]
  (-> query
      lib.serialize/prepare-for-serialization
      json-wire-value))

(defn exact-query-fingerprint
  "SHA-256 over deterministic canonical JSON of the exact serialized pMBQL."
  [query]
  (sha256-hex (canonical-json (exact-serialized-query query))))

(defn- type-name [value]
  (cond
    (keyword? value) (u/qualified-name value)
    (string? value) value
    (nil? value) nil
    :else (str value)))

(defn- block-type= [part expected]
  (= expected (type-name (:type part))))

(defn- structured-output [part]
  (or (get-in part [:result :structured-output])
      (get-in part [:result :structured_output])))

(defn- query-id-from-output [part]
  (:query-id (structured-output part)))

(defn- query-from-output [part]
  (:query (structured-output part)))

(defn- tool-input-by-id [message]
  (into {}
        (keep (fn [part]
                (when (block-type= part "tool-input")
                  [(:id part) part])))
        (:data message)))

(defn- matching-query-outputs [message native-query-id]
  (let [inputs (tool-input-by-id message)]
    (for [part (:data message)
          :when (block-type= part "tool-output")
          :when (= native-query-id (query-id-from-output part))]
      {:message message
       :output part
       :input (get inputs (:id part))})))

(defn- successful-query-output? [input output]
  (and input
       (contains? metabot.tools/query-generation-tool-names (:function input))
       (nil? (:error output))
       (some? (query-id-from-output output))
       (map? (query-from-output output))))

(defn- material-query-count [message]
  (let [inputs (tool-input-by-id message)]
    (count
     (for [part (:data message)
           :when (block-type= part "tool-output")
           :let [input (get inputs (:id part))]
           :when (successful-query-output? input part)]
       part))))

(defn- conversation-state-query [conversation native-query-id]
  (let [state (:state conversation)]
    (or (get-in state [:queries native-query-id])
        (get-in state [:queries (keyword native-query-id)])
        (get-in state ["queries" native-query-id]))))

(defn- shared-conversation? [conversation messages]
  (let [originator (:user_id conversation)
        user-ids   (set (keep :user_id messages))
        slack?     (some some? ((juxt :slack_team_id :slack_channel_id :slack_thread_ts) conversation))]
    (or slack?
        (> (count user-ids) 1)
        (and (seq user-ids) (not= user-ids #{originator})))))

(defn- assert-certified-subject! [conversation]
  (let [current-user api/*current-user-id*
        originator   (:user_id conversation)]
    (when-not current-user
      (fail! "NATIVE_ATTESTATION_AUTHENTICATION_REQUIRED" 401
             "Native analytical attestation requires an authenticated Metabase subject"))
    (when-not originator
      (fail! "NATIVE_QUERY_PRODUCER_INVALID" 409
             "Conversation has no stable originator subject"))
    (when-not (= current-user originator)
      (fail! "NATIVE_ATTESTATION_SUBJECT_MISMATCH" 403
             "Current Metabase subject is not the certified query-producing conversation originator"
             {:current-user-id current-user
              :conversation-originator-id originator}))
    current-user))

(defn- assert-supported-conversation! [conversation messages]
  (when (shared-conversation? conversation messages)
    (fail! "SHARED_CONVERSATION_ATTESTATION_UNSUPPORTED" 409
           "P13B-v1 does not certify shared or Slack conversation attribution")))

(defn- load-conversation-identity! [conversation-id]
  (or (t2/select-one [:model/MetabotConversation
                      :id
                      :user_id
                      :slack_team_id
                      :slack_channel_id
                      :slack_thread_ts]
                     :id conversation-id)
      (fail! "NATIVE_QUERY_OCCURRENCE_NOT_FOUND" 404
             "Metabot conversation was not found")))

(defn- load-conversation-state! [conversation-id]
  (:state
   (t2/select-one [:model/MetabotConversation :state]
                  :id conversation-id)))

(defn load-occurrence! [conversation-id native-query-id]
  (let [conversation-identity (load-conversation-identity! conversation-id)
        subject               (assert-certified-subject! conversation-identity)
        messages              (vec (t2/select :model/MetabotMessage :conversation_id conversation-id))
        _                     (assert-supported-conversation! conversation-identity messages)
        conversation          (assoc conversation-identity
                                     :state (load-conversation-state! conversation-id))
        assistants            (filterv #(= :assistant (:role %)) messages)
        matches               (vec (mapcat #(matching-query-outputs % native-query-id) assistants))]
    (cond
      (empty? matches)
      (fail! "NATIVE_QUERY_OCCURRENCE_NOT_FOUND" 404
             "No persisted producer occurrence matches the requested native query id")

      (> (count matches) 1)
      (fail! "NATIVE_QUERY_OCCURRENCE_AMBIGUOUS" 409
             "More than one persisted producer occurrence matches the requested native query id"
             {:occurrence-count (count matches)}))

    (let [{:keys [message input output]} (first matches)
          producer-tool      (:function input)
          persisted-producer (query-from-output output)]
      (when-not (and (= "construct_notebook_query" producer-tool)
                     (true? (:finished message))
                     (nil? (:error message))
                     (nil? (:error output))
                     (map? persisted-producer))
        (fail! "NATIVE_QUERY_PRODUCER_INVALID" 409
               "Requested query id is not bound to one finalized successful construct_notebook_query occurrence"))
      (let [persisted-state (conversation-state-query conversation native-query-id)]
        (when-not (map? persisted-state)
          (fail! "NATIVE_QUERY_STATE_MISMATCH" 409
                 "Conversation state does not contain the persisted producer query"))
        (let [producer-query (restore-persisted-query persisted-producer)
              state-query    (restore-persisted-query persisted-state)]
          (when-not (= (exact-serialized-query producer-query)
                       (exact-serialized-query state-query))
            (fail! "NATIVE_QUERY_STATE_MISMATCH" 409
                   "Persisted producer query and conversation-state query disagree"))
          {:conversation conversation
           :message message
           :tool-call-id (:id output)
           :producer-tool producer-tool
           :query state-query
           :material-query-count (material-query-count message)
           :authenticated-subject subject})))))

