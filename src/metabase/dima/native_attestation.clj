(ns metabase.dima.native-attestation
  "P13B bounded native-query attestation and exact engine identity.

  This namespace observes existing Metabot persistence plus Metabase Lib/QP.
  It does not author, repair, normalize, execute, or semantically reinterpret queries."
  (:require
   [clojure.string :as str]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.dima.native-occurrence :as dima.occurrence]
   [metabase.dima.native-query-compat :as dima.compat]
   [metabase.query-permissions.impl :as query-perms]
   [metabase.query-processor :as qp]
   [metabase.query-processor.middleware.desugar :as qp.desugar]
   [metabase.query-processor.middleware.permissions :as qp.perms]
   [metabase.query-processor.preprocess :as qp.preprocess]
   [metabase.query-processor.store :as qp.store]
   [metabase.types.core]
   [metabase.util :as u]
   [toucan2.core :as t2])
  (:import
   (java.time LocalDate LocalDateTime OffsetDateTime ZonedDateTime)
   (java.util UUID)))

(set! *warn-on-reflection* true)

(def ^:dynamic *runtime-identity-override*
  "Compatibility binding for historical P13 tests. Production callers never bind this."
  nil)

(def ^:dynamic *env-reader*
  "Compatibility binding for deterministic P13 identity tests."
  #(System/getenv %))

(defn runtime-identity
  "P13 compatibility wrapper over the neutral engine runtime identity owner."
  []
  (binding [dima.occurrence/*runtime-identity-override* *runtime-identity-override*
            dima.occurrence/*env-reader* *env-reader*]
    (dima.occurrence/runtime-identity)))

(defn canonical-json
  "Frozen canonical JSON delegated to the neutral occurrence identity owner."
  [value]
  (dima.occurrence/canonical-json value))

(defn- sha256-hex [value]
  (dima.occurrence/sha256-hex value))

(defn exact-serialized-query
  "Exact serialized occurrence artifact delegated to the neutral identity owner."
  [query]
  (dima.occurrence/exact-serialized-query query))

(defn exact-query-fingerprint
  "Exact occurrence fingerprint delegated to the neutral identity owner."
  [query]
  (dima.occurrence/exact-query-fingerprint query))

(defn- type-name [value]
  (cond
    (keyword? value) (u/qualified-name value)
    (string? value) value
    (nil? value) nil
    :else (str value)))

(defn- load-occurrence!
  "P13 compatibility wrapper preserving historical attestation-facing error codes."
  [conversation-id native-query-id]
  (try
    (dima.occurrence/load-occurrence! conversation-id native-query-id)
    (catch clojure.lang.ExceptionInfo e
      (let [data (ex-data e)
            code (:dima/error-code data)
            p13-code (case code
                       "NATIVE_OCCURRENCE_AUTHENTICATION_REQUIRED"
                       "NATIVE_ATTESTATION_AUTHENTICATION_REQUIRED"
                       "NATIVE_OCCURRENCE_SUBJECT_MISMATCH"
                       "NATIVE_ATTESTATION_SUBJECT_MISMATCH"
                       "SHARED_CONVERSATION_OCCURRENCE_UNSUPPORTED"
                       "SHARED_CONVERSATION_ATTESTATION_UNSUPPORTED"
                       code)]
        (throw (ex-info (ex-message e)
                        (assoc data :dima/error-code p13-code)
                        e))))))

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

(defn- breakout-fact [query stage-number breakout-index breakout]
  (let [column        (lib/breakout-column query stage-number breakout)
        field-id      (:id column)
        field-type    (or (:effective-type column) (:base-type column))
        temporal-unit (lib/raw-temporal-bucket column)]
    (when-not (and (pos-int? field-id) field-type)
      (fail! "NATIVE_BREAKOUT_SHAPE_UNSUPPORTED" 422
             "P13D-v1 certifies physical-field breakouts only"
             {:stage-number stage-number
              :breakout-index breakout-index}))
    {:stage_number   stage-number
     :breakout_index breakout-index
     :field_id       field-id
     :field_type     (type-name field-type)
     :temporal_unit  (some-> temporal-unit type-name)}))

(defn- breakout-facts [query]
  (vec
   (mapcat
    (fn [stage-number]
      (keep-indexed
       (fn [breakout-index breakout]
         (breakout-fact query stage-number breakout-index breakout))
       (or (lib/breakouts query stage-number) [])))
    (stage-numbers query))))

(defn- aggregation-order-target-index [query stage-number target]
  (when (= :aggregation (first target))
    (let [target-uuid (nth target 2 nil)]
      (first
       (keep-indexed
        (fn [aggregation-index aggregation]
          (when (= target-uuid (get-in aggregation [1 :lib/uuid]))
            aggregation-index))
        (or (lib/aggregations query stage-number) []))))))

(defn- field-order-target-fact [query stage-number target]
  (when (= :field (first target))
    (let [columns (vec (lib/referenced-columns query stage-number target))]
      (when (= 1 (count columns))
        (let [column     (first columns)
              field-id   (:id column)
              field-type (or (:effective-type column) (:base-type column))]
          (when (and (pos-int? field-id) field-type)
            {:target_kind "field"
             :field_id field-id
             :field_type (type-name field-type)}))))))

(defn- order-by-target-fact [query stage-number target]
  (if-let [aggregation-index
           (aggregation-order-target-index query stage-number target)]
    {:target_kind "aggregation"
     :aggregation_index aggregation-index}
    (or (field-order-target-fact query stage-number target)
        {:target_kind "other"})))

(defn- order-by-fact [query stage-number order-index order-by]
  (let [[direction _opts target] order-by]
    (merge
     {:stage_number stage-number
      :order_index  order-index
      :direction    (type-name direction)}
     (order-by-target-fact query stage-number target))))

(defn- order-by-facts [query]
  (vec
   (mapcat
    (fn [stage-number]
      (keep-indexed
       (fn [order-index order-by]
         (order-by-fact query stage-number order-index order-by))
       (or (lib/order-bys query stage-number) [])))
    (stage-numbers query))))

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
    (and (vector? value)
         (= 4 (count value))
         (= :absolute-datetime (first value)))
    (literal-temporal-value (nth value 2))
    :else nil))

(defn- textual-column? [column]
  (when column
    (let [column-type (or (:effective-type column) (:base-type column))]
      (and column-type (isa? column-type :type/Text)))))

(defn- textual-equality-predicate [query stage-number filter-clause]
  (let [{:keys [operator column args]} (lib/filter-parts query stage-number filter-clause)
        value (first args)]
    ;; P13C-v1 intentionally certifies exactly one scalar string equality shape.
    ;; Unsupported non-temporal shapes remain visible through non_temporal_filter_count
    ;; but do not become guessed typed predicates.
    (when (and (= operator :=)
               (textual-column? column)
               (pos-int? (:id column))
               (= 1 (count args))
               (string? value))
      {:stage_number  stage-number
       :field_id      (:id column)
       :operator      (type-name operator)
       :literal_value value
       :field_type    (type-name (or (:effective-type column) (:base-type column)))})))

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
                            :temporal (temporal-predicate query stage-number filter-clause)
                            :textual-equality
                            (textual-equality-predicate query stage-number filter-clause)}))
                       (stage-numbers query)))]
    {:material_filter_count        (count items)
     :non_temporal_filter_count    (count (remove :temporal items))
     :temporal_predicates          (vec (keep :temporal items))
     :textual_equality_predicates  (vec (keep :textual-equality items))}))

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
        runtime-query     (dima.compat/hydrate-runtime-query! query)
        runtime           (runtime-identity)
        preprocessed      (preprocess-and-authorize! runtime-query)
        checked-table-ids (->> (query-perms/query->source-table-ids preprocessed) sort vec)
        implicit          (implicit-joins preprocessed)
        implicit-ids      (->> implicit (keep implicit-joined-table-id) distinct sort vec)
        metric-refs       (native-metric-references runtime-query)
        aggs              (attested-aggregation-facts runtime-query preprocessed metric-refs)
        breakouts         (breakout-facts runtime-query)
        ordering-facts    (order-by-facts runtime-query)
        observation-query (qp.desugar/desugar runtime-query)
        filters           (filter-facts observation-query)
        manifest-base     {:native_conversation_id        (str conversation_id)
                           :native_assistant_message_id   (:id message)
                           :native_tool_call_id           tool-call-id
                           :native_query_id               native_query_id
                           :producer_tool                 producer-tool
                           :exact_pmbql_fingerprint       fingerprint
                           :database_id                   (lib/database-id runtime-query)
                           :primary_source_table_id       (lib/primary-source-table-id runtime-query)
                           :referenced_source_table_ids   checked-table-ids
                           :aggregation_count             (count aggs)
                           :aggregations                   aggs
                           :native_metric_references       metric-refs
                           :breakout_count                (count breakouts)
                           :breakouts                     breakouts
                           :material_filter_count         (:material_filter_count filters)
                           :non_temporal_filter_count     (:non_temporal_filter_count filters)
                           :temporal_predicates           (:temporal_predicates filters)
                           :textual_equality_predicates   (:textual_equality_predicates filters)
                           :explicit_join_count           (explicit-join-count runtime-query)
                           :implicit_join_count           (count implicit)
                           :implicit_joined_table_ids     implicit-ids
                           :order_by_count                (count ordering-facts)
                           :order_bys                     ordering-facts
                           :limit                         (lib/current-limit runtime-query)
                           :stage_count                   (lib/stage-count runtime-query)
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
