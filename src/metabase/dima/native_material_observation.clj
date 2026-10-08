(ns metabase.dima.native-material-observation
  "R5 read-only material semantic observation for one persisted native query occurrence.

  Metabase Lib/QP owns representation normalization. This namespace reports only stable native
  identities and material analytical meaning needed by Platform to decide Evidence eligibility.
  It does not certify physical implementation, execute datasets, plan/repair queries, or own
  Dima business semantics."
  (:require
   [metabase.dima.native-occurrence :as dima.occurrence]
   [metabase.lib.core :as lib]
   [metabase.lib.equality :as lib.equality]
   [metabase.lib.expression :as lib.expression]
   [metabase.types.core]
   [metabase.util.json :as json])
  (:import
   (java.time Instant LocalDate LocalDateTime OffsetDateTime ZoneOffset)))

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
(declare scalar-leaves)

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

(defn- temporal-order-instant
  [value]
  (let [text (str value)]
    (or
     (try
       (Instant/parse text)
       (catch Exception _ nil))
     (try
       (.toInstant (OffsetDateTime/parse text))
       (catch Exception _ nil))
     (try
       (.toInstant (LocalDateTime/parse text) ZoneOffset/UTC)
       (catch Exception _ nil))
     (try
       (-> (LocalDate/parse text)
           (.atStartOfDay ZoneOffset/UTC)
           .toInstant)
       (catch Exception _ nil)))))

(defn- single-scalar
  [value]
  (let [leaves (vec (scalar-leaves value))]
    (when (= 1 (count leaves))
      (first leaves))))

(defn- half-open-temporal-interval
  [predicate]
  (when (expression-parts? predicate)
    (let [clauses (if (= :and (:operator predicate))
                    (:args predicate)
                    [predicate])
          bounds
          (keep
           (fn [clause]
             (when (and (expression-parts? clause)
                        (#{:>= :<} (:operator clause)))
               (let [[column value & more] (:args clause)
                     scalar (single-scalar value)
                     instant (when (some? scalar)
                               (temporal-order-instant scalar))]
                 (when (and (empty? more)
                            (map? column)
                            (temporal-column? column)
                            (pos-int? (:id column))
                            instant)
                   {:operator (:operator clause)
                    :field_id (:id column)
                    :table_id (:table-id column)
                    :instant instant}))))
           clauses)
          identities (set (map (juxt :field_id :table_id) bounds))
          lowers (filter #(= :>= (:operator %)) bounds)
          uppers (filter #(= :< (:operator %)) bounds)]
      (when (and (= (count clauses) (count bounds))
                 (= 1 (count identities))
                 (= 1 (count lowers))
                 (= 1 (count uppers)))
        (let [lower (:instant (first lowers))
              upper (:instant (first uppers))
              [field-id table-id] (first identities)]
          (when (neg? (compare lower upper))
            (cond-> {:time_field_id field-id
                     :lower lower
                     :upper upper}
              (pos-int? table-id) (assoc :table_id table-id))))))))

(defn- bucket-upper-instant
  [lower grain]
  (let [zdt (.atZone ^Instant lower ZoneOffset/UTC)
        upper
        (case grain
          :minute (.plusMinutes zdt 1)
          :hour (.plusHours zdt 1)
          :day (.plusDays zdt 1)
          :week (.plusWeeks zdt 1)
          :month (.plusMonths zdt 1)
          :quarter (.plusMonths zdt 3)
          :year (.plusYears zdt 1)
          nil)]
    (when upper
      (.toInstant upper))))

(defn- bucketed-temporal-equality-interval
  [predicate]
  (when (and (expression-parts? predicate)
             (= := (:operator predicate)))
    (let [[column value & more] (:args predicate)
          scalar (single-scalar value)
          lower (when (some? scalar)
                  (temporal-order-instant scalar))
          grain (when (map? column)
                  (or (lib/raw-temporal-bucket column)
                      (:inherited-temporal-unit column)))
          upper (when (and lower grain)
                  (bucket-upper-instant lower grain))]
      (when (and (empty? more)
                 (map? column)
                 (temporal-column? column)
                 (pos-int? (:id column))
                 lower
                 upper
                 (neg? (compare lower upper)))
        (cond-> {:time_field_id (:id column)
                 :lower lower
                 :upper upper}
          (pos-int? (:table-id column))
          (assoc :table_id (:table-id column)))))))

(defn- temporal-period-interval
  [predicate]
  (or (half-open-temporal-interval predicate)
      (bucketed-temporal-equality-interval predicate)))

(defn- unique-source-uuid-match
  [source-uuid candidates]
  (when source-uuid
    (let [matches (vec
                   (filter
                    #(= source-uuid (:lib/source-uuid %))
                    candidates))]
      (when (= 1 (count matches))
        (first matches)))))

(defn- canonical-previous-stage-source-uuid
  [query stage-number column]
  (when-let [source-alias (:lib/source-column-alias column)]
    (let [previous-columns (lib/returned-columns query (dec stage-number))
          alias-matches
          (vec
           (filter
            #(= source-alias (:lib/desired-column-alias %))
            previous-columns))]
      (when (= 1 (count alias-matches))
        (:lib/source-uuid (first alias-matches))))))

(defn- matched-previous-stage-column
  [query stage-number column candidates]
  (when (and (pos? stage-number)
             (map? column)
             (= :source/previous-stage (:lib/source column))
             (seq candidates))
    (or
     (unique-source-uuid-match (:lib/source-uuid column) candidates)
     (unique-source-uuid-match
      (canonical-previous-stage-source-uuid query stage-number column)
      candidates)
     (lib.equality/find-matching-column
      query
      (dec stage-number)
      column
      candidates))))

(defn- previous-stage-aggregation
  [query stage-number column]
  (when (pos? stage-number)
    (let [previous-stage (dec stage-number)
          aggregations (or (lib/aggregations query previous-stage) [])
          metadata (or (lib/aggregations-metadata query previous-stage) [])
          matched (matched-previous-stage-column
                   query stage-number column metadata)
          source-uuid (:lib/source-uuid matched)]
      (when source-uuid
        (some
         (fn [[aggregation column-metadata]]
           (when (= source-uuid (:lib/source-uuid column-metadata))
             {:stage_number previous-stage
              :aggregation aggregation}))
         (map vector aggregations metadata))))))

(defn- previous-stage-governed-metric
  [query stage-number metric-index column]
  (when (pos? stage-number)
    (let [previous-stage (dec stage-number)
          metadata (or (lib/aggregations-metadata query previous-stage) [])
          matched (matched-previous-stage-column
                   query stage-number column metadata)
          source-uuid (:lib/source-uuid matched)]
      (when source-uuid
        (get metric-index [previous-stage source-uuid])))))

(defn- previous-stage-source-value
  [query stage-number column]
  (when (and (pos? stage-number)
             (map? column)
             (= :source/previous-stage (:lib/source column)))
    (let [previous-stage (dec stage-number)
          columns (or (lib/returned-columns query previous-stage) [])
          matched (matched-previous-stage-column
                   query stage-number column columns)]
      (case (:lib/source matched)
        :source/aggregations
        (some-> (previous-stage-aggregation query stage-number column)
                :aggregation)

        :source/expressions
        (when-let [expression-name
                   (or (:lib/expression-name matched)
                       (:name matched)
                       (:lib/source-column-alias matched))]
          (lib.expression/maybe-resolve-expression
           query previous-stage expression-name))

        nil))))

(declare material-lineage-fact)
(declare same-temporal-axis?)
(declare later-period?)

(defn- case-period-lineage-fact
  [query stage-number metric-index value]
  (when (and (vector? value)
             (#{:case :if} (first value)))
    (let [[_tag _opts cases fallback] value]
      (when (and (= 1 (count cases))
                 (number? fallback)
                 (zero? fallback))
        (let [[predicate measure] (first cases)
              predicate-parts
              (lib/expression-parts query stage-number predicate)
              interval (temporal-period-interval predicate-parts)
              measure-fact
              (material-lineage-fact
               query stage-number metric-index measure)]
          (when (and interval
                     (= :metric (:kind measure-fact)))
            {:kind :period-metric
             :metric (:metric measure-fact)
             :interval interval}))))))

(defn- difference-lineage-fact
  [left right]
  (when (and (= :period-metric (:kind left))
             (= :period-metric (:kind right))
             (same-metric? (:metric left) (:metric right))
             (same-temporal-axis? left right)
             (later-period? left right))
    {:kind :difference
     :metric (:metric left)
     :comparison (:interval left)
     :baseline (:interval right)}))

(defn- material-lineage-fact
  "Project stable analytical lineage from Lib structures without comparing it
  to any Dima intent. This returns facts only; fulfillment remains Product-owned."
  [query stage-number metric-index value]
  (or
   (case-period-lineage-fact query stage-number metric-index value)

   (when (and (map? value)
              (= :metadata/metric (:lib/type value)))
     (when-let [metric (stable-metric-identity value)]
       {:kind :metric :metric metric}))

   (when (and (vector? value)
              (#{:field :aggregation :expression} (first value)))
     (when-let [column
                (lib.equality/find-matching-column
                 query
                 stage-number
                 value
                 (or (lib/visible-columns query stage-number) []))]
       (material-lineage-fact
        query stage-number metric-index column)))

   (when (and (map? value)
              (= :source/previous-stage (:lib/source value))
              (pos? stage-number))
     (or
      (when-let [metric
                 (previous-stage-governed-metric
                  query stage-number metric-index value)]
        {:kind :metric :metric metric})
      (when-let [source
                 (previous-stage-source-value query stage-number value)]
        (material-lineage-fact
         query (dec stage-number) metric-index source))))

   (let [parts (try
                 (lib/expression-parts query stage-number value)
                 (catch Exception _ nil))]
     (when (expression-parts? parts)
       (let [operator (:operator parts)
             args (:args parts)]
         (cond
           (= :metadata/metric (:lib/type parts))
           (when-let [metric (stable-metric-identity parts)]
             {:kind :metric :metric metric})

           (and (= :sum operator) (= 1 (count args)))
           (material-lineage-fact
            query stage-number metric-index (first args))

           (and (= :sum-where operator) (= 2 (count args)))
           (let [[measure predicate] args
                 measure-fact
                 (material-lineage-fact
                  query stage-number metric-index measure)
                 interval (temporal-period-interval predicate)]
             (when (and (= :metric (:kind measure-fact))
                        interval)
               {:kind :period-metric
                :metric (:metric measure-fact)
                :interval interval}))

           (and (= :- operator) (= 2 (count args)))
           (difference-lineage-fact
            (material-lineage-fact
             query stage-number metric-index (first args))
            (material-lineage-fact
             query stage-number metric-index (second args)))

           :else nil))))))

(defn- wire-temporal-scope
  [interval]
  (cond-> {:time_field_id (:time_field_id interval)
           :lower_bound (wire-value (:lower interval))
           :lower_inclusive true
           :upper_bound (wire-value (:upper interval))
           :upper_inclusive false}
    (pos-int? (:table_id interval))
    (assoc :table_id (:table_id interval))))

(defn- period-aggregate-observation
  [query stage-number metric-index column]
  (when-let [{aggregate-stage :stage_number
              aggregation :aggregation}
             (previous-stage-aggregation query stage-number column)]
    (let [parts (lib/expression-parts query aggregate-stage aggregation)]
      (when (and (expression-parts? parts)
                 (= :sum-where (:operator parts)))
        (let [[metric-column predicate & more] (:args parts)
              metric (previous-stage-governed-metric
                      query aggregate-stage metric-index metric-column)
              interval (temporal-period-interval predicate)]
          (when (and (empty? more) metric interval)
            {:metric metric
             :interval interval}))))))

(defn- same-temporal-axis?
  [left right]
  (= (select-keys (:interval left) [:time_field_id :table_id])
     (select-keys (:interval right) [:time_field_id :table_id])))

(defn- later-period?
  [later earlier]
  (let [later-lower (get-in later [:interval :lower])
        earlier-lower (get-in earlier [:interval :lower])
        earlier-upper (get-in earlier [:interval :upper])]
    (and later-lower earlier-lower earlier-upper
         (neg? (compare earlier-lower later-lower))
         (not (pos? (compare earlier-upper later-lower))))))

(defn- period-pair-change-metric
  [query stage-number metric-index value]
  (when (and (expression-parts? value)
             (= :- (:operator value)))
    (let [[comparison-ref baseline-ref & more] (:args value)
          comparison (period-aggregate-observation
                      query stage-number metric-index comparison-ref)
          baseline (period-aggregate-observation
                    query stage-number metric-index baseline-ref)]
      (when (and (empty? more)
                 comparison
                 baseline
                 (same-metric? (:metric comparison) (:metric baseline))
                 (same-temporal-axis? comparison baseline)
                 (later-period? comparison baseline))
        (:metric comparison)))))

(defn- leading-temporal-breakout?
  [query stage-number]
  (when-let [breakout (first (or (lib/breakouts query stage-number) []))]
    (let [column (lib/breakout-column query stage-number breakout)]
      (boolean
       (and (temporal-column? column)
            (lib/raw-temporal-bucket column))))))

(defn- offset-change-ranking-index
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


(defn- change-ranking-index
  [query]
  (offset-change-ranking-index query))
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

(defn- resolved-order-by-target
  [query stage-number order-by]
  (let [target (nth order-by 2 nil)]
    (if (and (vector? target)
             (= :expression (first target)))
      (lib.expression/resolve-expression query stage-number (last target))
      target)))

(defn- order-by-lineage-fact
  [query stage-number metric-index order-by]
  (when-let [target (resolved-order-by-target query stage-number order-by)]
    (material-lineage-fact
     query stage-number metric-index target)))

(defn- order-by-period-pair-change-metric
  [query stage-number metric-index order-by]
  (when-let [target (resolved-order-by-target query stage-number order-by)]
    (period-pair-change-metric
     query
     stage-number
     metric-index
     (lib/expression-parts query stage-number target))))


(defn- order-by-reference-column
  "Resolve only ref-shaped ORDER BY targets to a visible column.

  Inline arithmetic/expression targets are valid Metabase HOW and have no
  standalone column ref. Their analytical lineage is derived structurally by
  order-by-lineage-fact instead of forcing them through ref-only Lib APIs."
  [query stage-number order-by]
  (let [target (nth order-by 2 nil)]
    (when (and (vector? target)
               (#{:field :aggregation :expression} (first target)))
      (lib.equality/find-matching-column
       query
       stage-number
       target
       (or (lib/visible-columns query stage-number) [])))))

(defn- ranking-observations [query metric-index change-index]
  (vec
   (mapcat
    (fn [stage-number]
      (let [order-bys (or (lib/order-bys query stage-number) [])
            limit-value (lib/current-limit query stage-number)]
        (map-indexed
         (fn [index order-by]
           (let [column (order-by-reference-column
                         query stage-number order-by)
                 direction (first order-by)
                 source-key (when column
                              [stage-number (:lib/source-uuid column)])
                 lineage-fact
                 (order-by-lineage-fact
                  query stage-number metric-index order-by)
                 lineage-change?
                 (= :difference (:kind lineage-fact))
                 change-metric (or
                                (when lineage-change?
                                  (:metric lineage-fact))
                                (order-by-period-pair-change-metric
                                 query stage-number metric-index order-by)
                                (when source-key
                                  (get change-index source-key)))
                 metric (or
                         (when (= :metric (:kind lineage-fact))
                           (:metric lineage-fact))
                         (when source-key
                           (get metric-index source-key)))
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
               (some? limit-value)
               (assoc :limit limit-value)

               lineage-change?
               (assoc :change_periods
                      {:baseline
                       (wire-temporal-scope (:baseline lineage-fact))
                       :comparison
                       (wire-temporal-scope (:comparison lineage-fact))}))))
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
        change-index    (change-ranking-index original)
        filters         (filter-observations original)
        ranking         (ranking-observations original metric-index change-index)
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
