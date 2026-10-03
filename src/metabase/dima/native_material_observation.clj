(ns metabase.dima.native-material-observation
  "R5 read-only material semantic observation for one persisted native query occurrence.

  Metabase Lib/QP owns representation normalization. This namespace reports only stable native
  identities and material analytical meaning needed by Platform to decide Evidence eligibility.
  It does not certify physical implementation, execute datasets, plan/repair queries, or own
  Dima business semantics."
  (:require
   [metabase.dima.native-occurrence :as dima.occurrence]
   [metabase.lib.core :as lib]
   [metabase.lib.expression :as lib.expression]
   [metabase.lib.ref :as lib.ref]
   [metabase.types.core]
   [metabase.util.json :as json]))

(set! *warn-on-reflection* true)

(defn- fail!
  ([code message]
   (fail! code 422 message nil))
  ([code message data]
   (fail! code 422 message data))
  ([code status message data]
   (throw (ex-info message
                   (merge {:status-code status
                           :dima/error-code code}
                          data)))))

(defn- wire-value [value]
  (json/decode (json/encode value)))

(defn- stage-numbers [query]
  (range (lib/stage-count query)))

(defn- require-positive-id [kind value]
  (when-not (pos-int? value)
    (fail! "NATIVE_MATERIAL_IDENTITY_UNSUPPORTED"
           (str "Material " kind " has no stable positive native id")))
  value)

(defn- column-identity [column]
  (let [field-id (require-positive-id "field" (:id column))
        table-id (:table-id column)]
    (cond-> {:field_id field-id}
      (pos-int? table-id) (assoc :table_id table-id))))

(defn- temporal-column? [column]
  (let [column-type (or (:effective-type column) (:base-type column))]
    (boolean (and column-type (isa? column-type :type/Temporal)))))

(defn- metric-observations [query]
  (vec
   (mapcat
    (fn [stage-number]
      (keep-indexed
       (fn [aggregation-index aggregation]
         (let [parts (lib/expression-parts query stage-number aggregation)]
           (when (= :metadata/metric (:lib/type parts))
             (let [metric-id (:id parts)
                   entity-id (:entity-id parts)]
               (when-not (and (pos-int? metric-id)
                              (string? entity-id)
                              (not-empty entity-id))
                 (fail! "NATIVE_MATERIAL_METRIC_IDENTITY_INVALID"
                        "Native metric has no stable Metabase id/entity-id"
                        {:stage_number stage-number
                         :aggregation_index aggregation-index}))
               {:stage_number stage-number
                :aggregation_index aggregation-index
                :metabase_metric_id metric-id
                :metabase_metric_entity_id entity-id}))))
       (or (lib/aggregations query stage-number) [])))
    (stage-numbers query))))

(defn- metric-ranking-index [query metric-observations]
  (into {}
        (keep
         (fn [{:keys [stage_number aggregation_index] :as metric}]
           (let [metadata (nth (or (lib/aggregations-metadata query stage_number) [])
                               aggregation_index
                               nil)
                 source-uuid (:lib/source-uuid metadata)]
             (when source-uuid
               [[stage_number source-uuid]
                (select-keys metric
                             [:metabase_metric_id
                              :metabase_metric_entity_id])]))))
        metric-observations))

(defn- metric-source-index
  [query metric-observations]
  (into {}
        (keep
         (fn [{:keys [stage_number aggregation_index] :as metric}]
           (let [metadata (nth (or (lib/aggregations-metadata query stage_number) [])
                               aggregation_index
                               nil)
                 source-uuid (:lib/source-uuid metadata)]
             (when source-uuid
               [source-uuid
                (select-keys metric
                             [:metabase_metric_id
                              :metabase_metric_entity_id])]))))
        metric-observations))

(defn- stable-metric-identity
  [value]
  (when (and (map? value)
             (= :metadata/metric (:lib/type value)))
    (let [metric-id (:id value)
          entity-id (:entity-id value)]
      (when-not (and (pos-int? metric-id)
                     (string? entity-id)
                     (not-empty entity-id))
        (fail! "NATIVE_MATERIAL_METRIC_IDENTITY_INVALID"
               "Derived ranking metric has no stable Metabase id/entity-id"))
      {:metabase_metric_id metric-id
       :metabase_metric_entity_id entity-id})))

(defn- expression-parts?
  [value]
  (and (map? value)
       (= :mbql/expression-parts (:lib/type value))))

(defn- same-metric?
  [left right]
  (and left right (= left right)))

(defn- previous-period-metric
  [value]
  (when (and (expression-parts? value)
             (= :offset (:operator value)))
    (let [[metric offset & more] (:args value)
          identity (stable-metric-identity metric)]
      (when (and (empty? more)
                 (= -1 offset)
                 identity)
        identity))))

(defn- ratio-to-previous-period-metric
  [value]
  (when (and (expression-parts? value)
             (= :/ (:operator value)))
    (let [[current previous & more] (:args value)
          current-id (stable-metric-identity current)
          previous-id (previous-period-metric previous)]
      (when (and (empty? more)
                 (same-metric? current-id previous-id))
        current-id))))

(defn- absolute-change-metric
  [value]
  (when (and (expression-parts? value)
             (= :- (:operator value)))
    (let [[current previous & more] (:args value)
          current-id (stable-metric-identity current)
          previous-id (previous-period-metric previous)]
      (when (and (empty? more)
                 (same-metric? current-id previous-id))
        current-id))))

(defn- percentage-change-metric
  [value]
  (when (and (expression-parts? value)
             (= :- (:operator value)))
    (let [[ratio one & more] (:args value)
          ratio-id (ratio-to-previous-period-metric ratio)]
      (when (and (empty? more)
                 ratio-id
                 (number? one)
                 (= 1.0 (double one)))
        ratio-id))))

(defn- delta-over-previous-metric
  [value]
  (when (and (expression-parts? value)
             (= :/ (:operator value)))
    (let [[delta previous & more] (:args value)
          delta-id (absolute-change-metric delta)
          previous-id (previous-period-metric previous)]
      (when (and (empty? more)
                 (same-metric? delta-id previous-id))
        delta-id))))

(declare change-ranking-metric)

(defn- scaled-change-metric
  [value]
  (when (and (expression-parts? value)
             (= :* (:operator value)))
    (let [[left right & more] (:args value)
          left-id (change-ranking-metric left)
          right-id (change-ranking-metric right)
          scalar-left? (and (number? left) (= 100.0 (double left)))
          scalar-right? (and (number? right) (= 100.0 (double right)))]
      (when (empty? more)
        (cond
          (and left-id scalar-right?) left-id
          (and right-id scalar-left?) right-id
          :else nil)))))

(defn- change-ranking-metric
  "Return the one governed metric identity for a structurally provable
  period-over-period change expression. This is observation only; it never
  rewrites or executes the query."
  [value]
  (or (absolute-change-metric value)
      (percentage-change-metric value)
      (delta-over-previous-metric value)
      (scaled-change-metric value)))

(defn- temporal-lineage-key
  [column]
  (when (and (map? column) (temporal-column? column))
    (cond
      ;; Persisted/rehydrated field refs may receive fresh query-local UUIDs.
      ;; Stable Metabase Field identity is stronger whenever it exists.
      (pos-int? (:id column))
      [:field-id (:table-id column) (:id column)]

      (:lib/source-uuid column)
      [:source-uuid (:lib/source-uuid column)]

      :else nil)))

(defn- temporal-bound-value
  [value]
  (cond
    (string? value)
    value

    (and (expression-parts? value)
         (= :absolute-datetime (:operator value))
         (= 2 (count (:args value))))
    (let [[raw-value unit] (:args value)]
      (when (and (string? raw-value)
                 (keyword? unit))
        raw-value))

    :else nil))

(defn- half-open-period-window
  [value]
  (when (and (expression-parts? value)
             (= :and (:operator value))
             (= 2 (count (:args value))))
    (let [bounds
          (keep
           (fn [part]
             (when (and (expression-parts? part)
                        (#{:>= :<} (:operator part))
                        (= 2 (count (:args part))))
               (let [[column raw-bound] (:args part)
                     lineage (temporal-lineage-key column)
                     bound (temporal-bound-value raw-bound)]
                 (when (and lineage bound)
                   {:operator (:operator part)
                    :lineage lineage
                    :bound bound}))))
           (:args value))
          lower (first (filter #(= :>= (:operator %)) bounds))
          upper (first (filter #(= :< (:operator %)) bounds))]
      (when (and (= 2 (count bounds))
                 lower
                 upper
                 (= (:lineage lower) (:lineage upper))
                 (neg? (compare (:bound lower) (:bound upper))))
        {:temporal_lineage (:lineage lower)
         :lower_bound (:bound lower)
         :upper_bound (:bound upper)}))))

(defn- governed-metric-for-material-column
  [query stage-number measure metric-by-source]
  (when (map? measure)
    (or
     (get metric-by-source (:lib/source-uuid measure))
     ;; After app-DB JSON round-trip, a previous-stage aggregation can be
     ;; represented as a name-based field ref with a fresh query-local UUID.
     ;; Use Lib's ambiguity-aware column matcher to recover the canonical
     ;; previous-stage output, then read only its stable source lineage.
     (when (pos? stage-number)
       (let [previous-stage (dec stage-number)
             columns (vec (or (lib/returned-columns query previous-stage) []))
             matched (lib/find-matching-column
                      query previous-stage measure columns)]
         (when matched
           (get metric-by-source (:lib/source-uuid matched))))))))

(defn- conditional-period-aggregation
  [query stage-number parts metric-by-source]
  (when (and (expression-parts? parts)
             (= :sum-where (:operator parts))
             (= 2 (count (:args parts))))
    (let [[measure condition] (:args parts)
          metric (governed-metric-for-material-column
                  query stage-number measure metric-by-source)
          window (half-open-period-window condition)]
      (when (and metric window)
        {:metric metric
         :window window}))))

(defn- conditional-period-index
  [query metric-by-source]
  (into {}
        (mapcat
         (fn [stage-number]
           (keep-indexed
            (fn [aggregation-index aggregation]
              (let [parts (lib/expression-parts query stage-number aggregation)
                    period (conditional-period-aggregation
                            query stage-number parts metric-by-source)
                    metadata (nth (or (lib/aggregations-metadata query stage-number) [])
                                  aggregation-index
                                  nil)
                    source-uuid (:lib/source-uuid metadata)]
                (when (and period source-uuid)
                  [source-uuid period])))
            (or (lib/aggregations query stage-number) [])))
         (stage-numbers query))))

(defn- adjacent-period-delta-metric
  [parts conditional-periods]
  (when (and (expression-parts? parts)
             (= :- (:operator parts))
             (= 2 (count (:args parts))))
    (let [[comparison-ref baseline-ref] (:args parts)
          comparison (when (map? comparison-ref)
                       (get conditional-periods (:lib/source-uuid comparison-ref)))
          baseline (when (map? baseline-ref)
                     (get conditional-periods (:lib/source-uuid baseline-ref)))
          comparison-window (:window comparison)
          baseline-window (:window baseline)]
      (when (and comparison
                 baseline
                 (same-metric? (:metric comparison) (:metric baseline))
                 (= (:temporal_lineage comparison-window)
                    (:temporal_lineage baseline-window))
                 (= (:upper_bound baseline-window)
                    (:lower_bound comparison-window))
                 (neg? (compare (:lower_bound baseline-window)
                                (:upper_bound baseline-window)))
                 (neg? (compare (:lower_bound comparison-window)
                                (:upper_bound comparison-window))))
        (:metric comparison)))))

(defn- conditional-change-ranking-metric
  [query stage-number order-by conditional-periods]
  (let [[_direction _options target-ref] order-by
        expression-name (when (= :expression (lib/dispatch-value target-ref))
                          (lib.ref/expression-ref-name target-ref))
        definition (when expression-name
                     (lib.expression/maybe-resolve-expression
                      query stage-number expression-name))
        parts (when definition
                (lib/expression-parts query stage-number definition))]
    ;; Expression names are only Metabase's query-local reference mechanism.
    ;; CHANGE authority comes exclusively from the resolved Lib structure:
    ;; same governed metric, adjacent half-open periods, comparison - baseline.
    (when parts
      (adjacent-period-delta-metric parts conditional-periods))))

(defn- leading-temporal-breakout?
  [query stage-number]
  (when-let [breakout (first (or (lib/breakouts query stage-number) []))]
    (let [column (lib/breakout-column query stage-number breakout)]
      (boolean
       (and (temporal-column? column)
            (lib/raw-temporal-bucket column))))))

(defn- change-ranking-index
  [query]
  (into {}
        (mapcat
         (fn [stage-number]
           (let [period-over-period? (leading-temporal-breakout? query stage-number)]
             (keep-indexed
              (fn [aggregation-index aggregation]
                (let [parts (lib/expression-parts query stage-number aggregation)
                      metric (when period-over-period?
                               (change-ranking-metric parts))
                      metadata (nth (or (lib/aggregations-metadata query stage-number) [])
                                    aggregation-index
                                    nil)
                      source-uuid (:lib/source-uuid metadata)]
                  (when (and metric source-uuid)
                    [[stage-number source-uuid] metric])))
              (or (lib/aggregations query stage-number) []))))
         (stage-numbers query))))

(defn- breakout-dimensions [query]
  (vec
   (mapcat
    (fn [stage-number]
      (map
       (fn [breakout]
         (let [column (lib/breakout-column query stage-number breakout)
               grain  (lib/raw-temporal-bucket column)]
           (cond-> (merge {:stage_number stage-number
                           :role "breakout"}
                          (column-identity column))
             grain (assoc :temporal_grain (name grain)))))
       (or (lib/breakouts query stage-number) [])))
    (stage-numbers query))))

(defn- scalar-leaves [value]
  (cond
    (and (map? value) (= :mbql/expression-parts (:lib/type value)))
    (mapcat scalar-leaves (:args value))

    (map? value)
    []

    (and (sequential? value) (not (string? value)))
    (mapcat scalar-leaves value)

    (keyword? value)
    []

    :else
    [(wire-value value)]))

(defn- semantic-literal [query stage-number value]
  (let [parts  (lib/expression-parts query stage-number value)
        leaves (vec (scalar-leaves parts))]
    (when-not (= 1 (count leaves))
      (fail! "NATIVE_MATERIAL_FILTER_VALUE_UNSUPPORTED"
             "Metabase semantic filter parts do not expose exactly one material scalar value"
             {:stage_number stage-number
              :value_count (count leaves)}))
    (first leaves)))

(defn- filter-observations [query]
  (vec
   (mapcat
    (fn [stage-number]
      (map
       (fn [clause]
         (let [{:keys [operator column args]} (lib/filter-parts query stage-number clause)]
           (when-not (and operator column)
             (fail! "NATIVE_MATERIAL_FILTER_UNSUPPORTED"
                    "Metabase Lib could not expose a material filter column/operator"
                    {:stage_number stage-number}))
           (let [identity (column-identity column)
                 values   (mapv #(semantic-literal query stage-number %) args)]
             (merge {:stage_number stage-number
                     :operator (name operator)
                     :values values
                     :temporal (temporal-column? column)}
                    identity))))
       (or (lib/atomic-filters query stage-number) [])))
    (stage-numbers query))))

(defn- merge-bound [scope side value inclusive]
  (let [value-key (keyword (str (name side) "_bound"))
        incl-key  (keyword (str (name side) "_inclusive"))]
    (when (contains? scope value-key)
      (fail! "NATIVE_MATERIAL_TEMPORAL_SCOPE_AMBIGUOUS"
             "More than one canonical temporal bound exists on the same side"
             {:field_id (:time_field_id scope)
              :bound_side (name side)}))
    (assoc scope value-key value incl-key inclusive)))

(defn- temporal-predicate->scope [scope {:keys [operator values]}]
  (case operator
    ">=" (merge-bound scope :lower (first values) true)
    ">"  (merge-bound scope :lower (first values) false)
    "<=" (merge-bound scope :upper (first values) true)
    "<"  (merge-bound scope :upper (first values) false)
    "="  (-> scope
             (merge-bound :lower (first values) true)
             (merge-bound :upper (first values) true))
    "between"
    (-> scope
        (merge-bound :lower (first values) true)
        (merge-bound :upper (second values) true))
    (fail! "NATIVE_MATERIAL_TEMPORAL_OPERATOR_UNSUPPORTED"
           "Metabase normalized temporal filter uses an unsupported material operator"
           {:operator operator
            :field_id (:time_field_id scope)})))

(defn- temporal-scopes [filter-observations]
  (let [temporal (filter :temporal filter-observations)]
    (->> temporal
         (group-by (juxt :field_id :table_id))
         (map
          (fn [[[field-id table-id] predicates]]
            (reduce
             temporal-predicate->scope
             (cond-> {:time_field_id field-id}
               (pos-int? table-id) (assoc :table_id table-id))
             predicates)))
         (sort-by (juxt :time_field_id :table_id))
         vec)))

(defn- filter-dimensions [filter-observations]
  (->> filter-observations
       (map (fn [item]
              (cond-> {:stage_number (:stage_number item)
                       :role (if (:temporal item) "temporal" "filter")
                       :field_id (:field_id item)}
                (pos-int? (:table_id item)) (assoc :table_id (:table_id item)))))
       distinct
       vec))

(defn- material-filters [filter-observations]
  (->> filter-observations
       (remove :temporal)
       (mapv #(dissoc % :temporal))))

(defn- ranking-observations
  [query metric-index change-index conditional-periods]
  (vec
   (mapcat
    (fn [stage-number]
      (let [columns (or (lib/orderable-columns query stage-number) [])
            by-position (into {}
                              (keep (fn [column]
                                      (when-some [position (:order-by-position column)]
                                        [position column])))
                              columns)
            order-bys (or (lib/order-bys query stage-number) [])
            limit-value (lib/current-limit query stage-number)]
        (map-indexed
         (fn [index order-by]
           (let [column (get by-position index)
                 info   (lib/display-info query stage-number order-by)
                 direction (:direction info)
                 source-key (when column
                              [stage-number (:lib/source-uuid column)])
                 conditional-change
                 (conditional-change-ranking-metric
                  query stage-number order-by conditional-periods)
                 change-metric (or conditional-change
                                   (when source-key
                                     (get change-index source-key)))
                 metric (when source-key
                          (get metric-index source-key))
                 field-id (:id column)
                 target (cond
                          change-metric
                          (assoc change-metric :kind "metric")

                          metric
                          (assoc metric :kind "metric")

                          (pos-int? field-id)
                          (cond-> {:kind "field" :field_id field-id}
                            (pos-int? (:table-id column))
                            (assoc :table_id (:table-id column)))

                          :else
                          (fail! "NATIVE_MATERIAL_RANKING_TARGET_UNSUPPORTED"
                                 "Ranking target has no stable native field or metric identity"
                                 {:stage_number stage-number
                                  :order_index index}))]
             (when-not (#{:asc :desc} direction)
               (fail! "NATIVE_MATERIAL_RANKING_DIRECTION_INVALID"
                      "Ranking direction is not a stable Metabase direction"
                      {:stage_number stage-number
                       :order_index index}))
             (cond-> {:stage_number stage-number
                      :order_index index
                      :target target
                      :direction (name direction)
                      :basis (if change-metric "change" "level")}
               (some? limit-value) (assoc :limit limit-value))))
         order-bys)))
    (stage-numbers query))))

(defn- ranking-dimensions [ranking]
  (->> ranking
       (keep (fn [{:keys [stage_number target]}]
               (when (= "field" (:kind target))
                 (cond-> {:stage_number stage_number
                          :role "ranking"
                          :field_id (:field_id target)}
                   (pos-int? (:table_id target))
                   (assoc :table_id (:table_id target))))))
       vec))

(defn observe-native-query-material!
  "Observe material native semantics for one already-persisted successful Metabot occurrence.

  The exact occurrence remains immutable. Material meaning is read from the already-restored
  Metabase Lib query owned by the neutral occurrence seam; exact serialization is used only by
  occurrence identity/fingerprinting. No dataset execution or QP preprocessing happens here."
  [{:keys [conversation_id native_query_id]}]
  (when-not (instance? java.util.UUID conversation_id)
    (fail! "NATIVE_MATERIAL_LOCATOR_INVALID" 400
           "conversation_id must be a UUID"))
  (when-not (and (string? native_query_id) (not-empty native_query_id))
    (fail! "NATIVE_MATERIAL_LOCATOR_INVALID" 400
           "native_query_id must be non-empty"))
  (let [occurrence      (dima.occurrence/load-occurrence!
                         (str conversation_id)
                         native_query_id)
        original        (:query occurrence)
        fingerprint     (dima.occurrence/exact-query-fingerprint original)
        metrics         (metric-observations original)
        metric-index    (metric-ranking-index original metrics)
        metric-by-source (metric-source-index original metrics)
        conditional-periods
        (conditional-period-index original metric-by-source)
        change-index    (change-ranking-index original)
        filters         (filter-observations original)
        ranking         (ranking-observations
                         original metric-index change-index conditional-periods)
        breakouts       (breakout-dimensions original)
        dimensions      (vec (distinct
                              (concat breakouts
                                      (filter-dimensions filters)
                                      (ranking-dimensions ranking))))
        database-id     (lib/database-id original)]
    (when-not (pos-int? database-id)
      (fail! "NATIVE_MATERIAL_DATABASE_ID_INVALID"
             "Observed query has no positive native database id"))
    {:schema_version "dima_native_material_observation_v1"
     :conversation_id (str conversation_id)
     :native_query_id native_query_id
     :assistant_message_id (get-in occurrence [:message :id])
     :tool_call_id (:tool-call-id occurrence)
     :query_fingerprint fingerprint
     :authenticated_metabase_subject (:authenticated-subject occurrence)
     :database_id database-id
     :runtime_identity (dima.occurrence/runtime-identity)
     :native_metrics metrics
     :dimensions dimensions
     :filters (material-filters filters)
     :temporal_scopes (temporal-scopes filters)
     :ranking ranking}))
