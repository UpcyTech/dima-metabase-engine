(ns metabase.dima.native-attestation
  "P13B bounded native-query attestation and exact engine identity.

  This namespace observes existing Metabot persistence plus Metabase Lib/QP.
  It does not author, repair, normalize, execute, or semantically reinterpret queries."
  (:require
   [clojure.string :as str]
   [metabase.api.common :as api]
   [metabase.lib-be.core :as lib-be]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.lib.serialize :as lib.serialize]
   [metabase.metabot.tools :as metabot.tools]
   [metabase.query-permissions.impl :as query-perms]
   [metabase.query-processor :as qp]
   [metabase.query-processor.middleware.desugar :as qp.desugar]
   [metabase.query-processor.middleware.permissions :as qp.perms]
   [metabase.query-processor.preprocess :as qp.preprocess]
   [metabase.query-processor.store :as qp.store]
   [metabase.types.core]
   [metabase.util :as u]
   [metabase.util.json :as json]
   [toucan2.core :as t2])
  (:import
   (java.math BigInteger)
   (java.nio.charset StandardCharsets)
   (java.security MessageDigest)
   (java.time LocalDate LocalDateTime OffsetDateTime ZonedDateTime)
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

(defn- sha256-hex [^String value]
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

(defn- load-occurrence! [conversation-id native-query-id]
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

(defn- stage-numbers [query]
  (range (lib/stage-count query)))

(defn- referenced-field-ids [query stage-number clause]
  (->> (lib/referenced-columns query stage-number clause)
       (keep :id)
       distinct
       sort
       vec))

(defn- aggregation-fact [query stage-number aggregation]
  (let [{:keys [operator]} (lib/expression-parts query stage-number aggregation)
        field-ids          (referenced-field-ids query stage-number aggregation)
        distinct?          (= operator :distinct)
        argument-kind      (cond
                             (and (= operator :count) (empty? field-ids)) "all_rows"
                             (and (= operator :count) (seq field-ids))   "field"
                             distinct?                                   "field"
                             (seq field-ids)                             "field_or_expression"
                             :else                                       "expression")]
    {:operator             (type-name operator)
     :argument_kind        argument-kind
     :referenced_field_ids field-ids
     :distinct             distinct?}))

(defn- aggregation-facts [query]
  (vec
   (mapcat (fn [stage-number]
             (map #(aggregation-fact query stage-number %)
                  (or (lib/aggregations query stage-number) [])))
           (stage-numbers query))))

(defn- native-metric-reference
  [query stage-number aggregation-index aggregation]
  (when (= :metric (first aggregation))
    (let [metric-id (nth aggregation 2 nil)]
      (when-not (pos-int? metric-id)
        (fail! "NATIVE_METRIC_REFERENCE_INVALID" 422
               "Native metric reference must resolve to a positive Metabase metric id"
               {:stage-number stage-number
                :aggregation-index aggregation-index}))
      (let [metric    (lib.metadata/metric query metric-id)
            entity-id (:entity-id metric)]
        (when-not (and (map? metric) (not (str/blank? entity-id)))
          (fail! "NATIVE_METRIC_REFERENCE_INVALID" 422
                 "Native metric reference has no stable Metabase entity identity"
                 {:stage-number stage-number
                  :aggregation-index aggregation-index
                  :metabase-metric-id metric-id}))
        {:stage_number stage-number
         :aggregation_index aggregation-index
         :metabase_metric_id metric-id
         :metabase_metric_entity_id entity-id}))))

(defn- native-metric-references [query]
  (vec
   (mapcat
    (fn [stage-number]
      (keep-indexed
       (fn [aggregation-index aggregation]
         (native-metric-reference query stage-number aggregation-index aggregation))
       (or (lib/aggregations query stage-number) [])))
    (stage-numbers query))))

(defn- attested-aggregation-facts
  [query preprocessed native-metric-refs]
  (if (empty? native-metric-refs)
    (aggregation-facts query)
    (let [original-count (count (aggregation-facts query))
          expanded       (aggregation-facts preprocessed)]
      ;; This bounded P13B seam certifies exactly one native metric aggregation.
      ;; Do not silently pair/flatten more complex metric algebra.
      (when-not (and (= 1 (count native-metric-refs))
                     (= 1 original-count)
                     (= 1 (count expanded)))
        (fail! "NATIVE_METRIC_EXPANSION_UNSUPPORTED" 422
               "P13B-v1 certifies one native metric aggregation only"
               {:native-metric-reference-count (count native-metric-refs)
                :original-aggregation-count original-count
                :expanded-aggregation-count (count expanded)}))
      expanded)))

(defn- breakout-count [query]
  (reduce + 0
          (for [stage-number (stage-numbers query)]
            (count (or (lib/breakouts query stage-number) [])))))

(defn- order-by-count [query]
  (reduce + 0
          (for [stage-number (stage-numbers query)]
            (count (or (lib/order-bys query stage-number) [])))))

(defn- explicit-join-count [query]
  (reduce + 0
          (for [stage-number (stage-numbers query)]
            (count (or (lib/joins query stage-number) [])))))

(defn- implicit-joins [preprocessed]
  (vec
   (mapcat (fn [stage-number]
             (filter :qp/is-implicit-join
                     (or (lib/joins preprocessed stage-number) [])))
           (stage-numbers preprocessed))))

(defn- implicit-joined-table-id [join]
  (get-in join [:stages 0 :source-table]))

(defn- temporal-column? [column]
  (when column
    (let [column-type (or (:effective-type column) (:base-type column))]
      (and column-type (isa? column-type :type/Temporal)))))

(defn- literal-temporal-value [value]
  (cond
    (string? value) value
    (instance? LocalDate value) (str value)
    (instance? LocalDateTime value) (str value)
    (instance? OffsetDateTime value) (str value)
    (instance? ZonedDateTime value) (str value)
    :else nil))

(defn- temporal-predicate [query stage-number filter-clause]
  (let [{:keys [operator column args options]} (lib/filter-parts query stage-number filter-clause)]
    (when (temporal-column? column)
      (let [values (mapv literal-temporal-value args)]
        (when (some nil? values)
          (fail! "NATIVE_TEMPORAL_SHAPE_UNSUPPORTED" 422
                 "P13B-v1 certifies only literal absolute temporal bounds"
                 {:operator (type-name operator)
                  :field-id (:id column)}))
        (let [[v1 v2] values
              base {:time_field_id      (:id column)
                    :operator           (type-name operator)
                    :lower_bound        nil
                    :upper_bound        nil
                    :lower_inclusive    nil
                    :upper_inclusive    nil
                    :field_temporal_type (type-name (or (:effective-type column) (:base-type column)))
                    :temporal_unit      (type-name (:temporal-unit options))}]
          (case operator
            :>       (assoc base :lower_bound v1 :lower_inclusive false)
            :>=      (assoc base :lower_bound v1 :lower_inclusive true)
            :<       (assoc base :upper_bound v1 :upper_inclusive false)
            :<=      (assoc base :upper_bound v1 :upper_inclusive true)
            :between (assoc base
                            :lower_bound v1 :lower_inclusive true
                            :upper_bound v2 :upper_inclusive true)
            :=       (assoc base
                            :lower_bound v1 :lower_inclusive true
                            :upper_bound v1 :upper_inclusive true)
            (fail! "NATIVE_TEMPORAL_SHAPE_UNSUPPORTED" 422
                   "P13B-v1 does not interpret this temporal operator"
                   {:operator (type-name operator)
                    :field-id (:id column)})))))))

(defn- filter-facts [query]
  (let [items (vec
               (mapcat (fn [stage-number]
                         (for [filter-clause (or (lib/atomic-filters query stage-number) [])]
                           {:stage stage-number
                            :clause filter-clause
                            :temporal (temporal-predicate query stage-number filter-clause)}))
                       (stage-numbers query)))]
    {:material_filter_count     (count items)
     :non_temporal_filter_count (count (remove :temporal items))
     :temporal_predicates       (vec (keep :temporal items))}))

(defn- preprocess-and-authorize! [query]
  ;; This performs the same native QP preprocessing and current-user permission check
  ;; without compiling for a driver or executing the analytical query. Permission
  ;; calculation requires the QP metadata store to stay bound across both operations.
  (qp.store/with-metadata-provider (lib/database-id query)
    (let [preprocessed (qp.preprocess/preprocess
                        (qp/userland-query-with-default-constraints query))]
      (qp.perms/check-query-permissions* preprocessed)
      preprocessed)))

(defn- attestation-id [{:keys [conversation-id assistant-message-id tool-call-id native-query-id fingerprint runtime-instance-id]}]
  (str "dima_att_"
       (subs
        (sha256-hex
         (str conversation-id "\u001f"
              assistant-message-id "\u001f"
              tool-call-id "\u001f"
              native-query-id "\u001f"
              fingerprint "\u001f"
              runtime-instance-id))
        0 24)))

(defn attest-native-query!
  "Attest one exact native query occurrence located only by conversation + query id.

  The query is retrieved from server-side persisted Metabot state. Caller-supplied semantic
  expectations are intentionally absent from this API."
  [{:keys [conversation_id native_query_id]}]
  (when-not (instance? UUID conversation_id)
    (fail! "NATIVE_QUERY_OCCURRENCE_INVALID_LOCATOR" 400
           "conversation_id must be a UUID"))
  (when (str/blank? native_query_id)
    (fail! "NATIVE_QUERY_OCCURRENCE_INVALID_LOCATOR" 400
           "native_query_id must be non-empty"))
  (let [{:keys [message tool-call-id producer-tool query material-query-count authenticated-subject]}
        (load-occurrence! (str conversation_id) native_query_id)
        exact-query       (exact-serialized-query query)
        fingerprint       (exact-query-fingerprint query)
        runtime           (runtime-identity)
        preprocessed      (preprocess-and-authorize! query)
        checked-table-ids (->> (query-perms/query->source-table-ids preprocessed) sort vec)
        implicit          (implicit-joins preprocessed)
        implicit-ids      (->> implicit (keep implicit-joined-table-id) distinct sort vec)
        metric-refs       (native-metric-references query)
        aggs              (attested-aggregation-facts query preprocessed metric-refs)
        observation-query (qp.desugar/desugar query)
        filters           (filter-facts observation-query)
        manifest-base     {:native_conversation_id        (str conversation_id)
                           :native_assistant_message_id   (:id message)
                           :native_tool_call_id           tool-call-id
                           :native_query_id               native_query_id
                           :producer_tool                 producer-tool
                           :exact_pmbql_fingerprint       fingerprint
                           :database_id                   (lib/database-id query)
                           :primary_source_table_id       (lib/primary-source-table-id query)
                           :referenced_source_table_ids   checked-table-ids
                           :aggregation_count             (count aggs)
                           :aggregations                   aggs
                           :native_metric_references       metric-refs
                           :breakout_count                (breakout-count query)
                           :material_filter_count         (:material_filter_count filters)
                           :non_temporal_filter_count     (:non_temporal_filter_count filters)
                           :temporal_predicates           (:temporal_predicates filters)
                           :explicit_join_count           (explicit-join-count query)
                           :implicit_join_count           (count implicit)
                           :implicit_joined_table_ids     implicit-ids
                           :order_by_count                (order-by-count query)
                           :limit                         (lib/current-limit query)
                           :stage_count                   (lib/stage-count query)
                           :material_query_count          material-query-count
                           :authenticated_metabase_subject authenticated-subject
                           :validation_provenance         {:producer_structured_output "PASSED"
                                                           :pmbql_schema               "PASSED"
                                                           :producer_query_id_match    "PASSED"
                                                           :producer_state_match       "PASSED"}
                           :permission_provenance         {:current_metabase_user_id  authenticated-subject
                                                           :permission_check          "PASSED"
                                                           :checked_source_table_ids  checked-table-ids}
                           :runtime_identity              runtime}
        manifest          (assoc manifest-base
                                 :attestation_id
                                 (attestation-id {:conversation-id (str conversation_id)
                                                  :assistant-message-id (:id message)
                                                  :tool-call-id tool-call-id
                                                  :native-query-id native_query_id
                                                  :fingerprint fingerprint
                                                  :runtime-instance-id (:runtime_instance_id runtime)}))]
    {:exact_serialized_pmbql exact-query
     :manifest manifest}))
