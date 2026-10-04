(ns metabase.dima.native-material-observation-test
  (:require
   [clojure.test :refer :all]
   [metabase.api.common :as api]
   [metabase.dima.native-attestation :as dima.attestation]
   [metabase.dima.native-material-observation :as dima.material]
   [metabase.dima.native-occurrence :as dima.occurrence]
   [metabase.lib.core :as lib]
   [metabase.lib.expression :as lib.expression]
   [metabase.lib.filter :as lib.filter]
   [metabase.query-processor :as qp]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.metabot.persistence :as metabot.persistence]
   [metabase.test :as mt]
   [metabase.test.fixtures :as fixtures])
  (:import
   (java.time LocalDate)))

(use-fixtures :once (fixtures/initialize :db))

(def ^:private test-runtime
  {:repository          "UpcyTech/dima-metabase-engine"
   :revision_sha        "1111111111111111111111111111111111111111"
   :upstream_base_sha   "2ba2485c78d7e00a9a25f82c00fc201da71590c4"
   :runtime_tag         "v0.63.18-dima.7-test"
   :build_identity      "test-build:r5"
   :image_identity      "local-image:r5"
   :runtime_instance_id "00000000-0000-4000-8000-000000000713"})

(defn- exception-code [f]
  (try (f) nil
       (catch clojure.lang.ExceptionInfo e
         (:dima/error-code (ex-data e)))))

(defn- thrown-value [f]
  (try
    (f)
    nil
    (catch Throwable t
      t)))

(deftest r8-material-observation-fail-helper-supports-typed-2-3-4-arity-test
  (let [fail-var (ns-resolve 'metabase.dima.native-material-observation 'fail!)
        cases [{:args ["R8_TWO" "two"]
                :status 422
                :data nil}
               {:args ["R8_THREE" "three" {:probe "r8"}]
                :status 422
                :data {:probe "r8"}}
               {:args ["R8_FOUR" 409 "four" {:probe "r8-explicit"}]
                :status 409
                :data {:probe "r8-explicit"}}]]
    (is (some? fail-var))
    (doseq [{:keys [args status data]} cases]
      (let [thrown (thrown-value #(apply fail-var args))]
        (is (instance? clojure.lang.ExceptionInfo thrown)
            (str "fail! must throw typed ExceptionInfo for arity " (count args)
                 ", got " (some-> thrown class .getName)))
        (when (instance? clojure.lang.ExceptionInfo thrown)
          (is (= status (:status-code (ex-data thrown))))
          (is (= (first args) (:dima/error-code (ex-data thrown))))
          (doseq [[k v] data]
            (is (= v (get (ex-data thrown) k)))))))))

(deftest r8-current-three-argument-fail-paths-do-not-leak-arity-exception-test
  (let [merge-bound-var
        (ns-resolve 'metabase.dima.native-material-observation 'merge-bound)
        temporal-predicate-var
        (ns-resolve 'metabase.dima.native-material-observation 'temporal-predicate->scope)
        cases [{:code "NATIVE_MATERIAL_TEMPORAL_SCOPE_AMBIGUOUS"
                :thunk #(merge-bound-var
                         {:time_field_id 7
                          :lower_bound "2026-06-01"
                          :lower_inclusive true}
                         :lower
                         "2026-06-15"
                         true)}
               {:code "NATIVE_MATERIAL_TEMPORAL_OPERATOR_UNSUPPORTED"
                :thunk #(temporal-predicate-var
                         {:time_field_id 7}
                         {:operator "during"
                          :values ["2026-06-01" "2026-07-01"]})}]]
    (is (some? merge-bound-var))
    (is (some? temporal-predicate-var))
    (doseq [{:keys [code thunk]} cases]
      (let [thrown (thrown-value thunk)]
        (is (instance? clojure.lang.ExceptionInfo thrown)
            (str code " must be typed ExceptionInfo, got "
                 (some-> thrown class .getName)))
        (when (instance? clojure.lang.ExceptionInfo thrown)
          (is (= code (:dima/error-code (ex-data thrown))))
          (is (= 422 (:status-code (ex-data thrown)))))))))

(defn- tool-parts
  [{:keys [query-id query call-id producer]
    :or {call-id "tool-r5-1" producer "construct_notebook_query"}}]
  [{:type :tool-input :id call-id :function producer :arguments {}}
   {:type :tool-output
    :id call-id
    :result {:output "ok"
             :structured-output {:query-id query-id :query query}}}])

(defn- persist-turn!
  [{:keys [conversation-id query-id query user-id state-query extra-parts]}]
  (let [{:keys [assistant-msg-id]}
        (metabot.persistence/start-turn!
         conversation-id "internal" {:role "user" :content "r5"} :user-id user-id)
        parts (vec
               (concat
                (tool-parts {:query-id query-id :query query})
                extra-parts
                [{:type :data
                  :data-type "state"
                  :data {:queries {query-id (or state-query query)}}}]))]
    (metabot.persistence/finalize-assistant-turn!
     conversation-id assistant-msg-id parts :finished? true)
    assistant-msg-id))

(defn- observe! [conversation-id query-id]
  (binding [dima.occurrence/*runtime-identity-override* test-runtime]
    (dima.material/observe-native-query-material!
     {:conversation_id (java.util.UUID/fromString conversation-id)
      :native_query_id query-id})))

(defn- metric-query [metric-id]
  (let [mp       (mt/metadata-provider)
        quantity (lib.metadata/field mp (mt/id :orders :quantity))]
    (-> (lib/query mp (lib.metadata/table mp (mt/id :orders)))
        (lib/aggregate (lib.metadata/metric mp metric-id))
        (lib/aggregate (lib/count))
        (lib/aggregate (lib/sum quantity)))))

(defn- metabase-absolute-date-source-query []
  (let [mp       (mt/metadata-provider)
        date-col (lib.metadata/field mp (mt/id :checkins :date))
        lower    (lib/absolute-datetime (LocalDate/parse "2026-06-01") :day)
        upper    (lib/absolute-datetime (LocalDate/parse "2026-07-01") :day)]
    (-> (lib/query mp (lib.metadata/table mp (mt/id :checkins)))
        (lib/aggregate (lib/count))
        (lib/filter (lib/>= date-col lower))
        (lib/filter (lib/< date-col upper)))))

(defn- v3-persisted-absolute-date-query []
  ;; Frozen provider-free equivalent of the V3 occurrence class: the persisted
  ;; query is produced by Metabase's own serializer from a legal Lib query.
  (dima.occurrence/exact-serialized-query
   (metabase-absolute-date-source-query)))

(defn- string-date-query []
  (let [mp (mt/metadata-provider)
        date-col (lib.metadata/field mp (mt/id :checkins :date))]
    (-> (lib/query mp (lib.metadata/table mp (mt/id :checkins)))
        (lib/aggregate (lib/count))
        (lib/filter (lib/>= date-col "2026-06-01"))
        (lib/filter (lib/< date-col "2026-07-01")))))

(defn- r8-timestamp-string-query []
  ;; Exact literal class observed in paid Panel A after Metabase persisted the
  ;; accepted May+June scope.
  (let [mp (mt/metadata-provider)
        date-col (lib.metadata/field mp (mt/id :checkins :date))]
    (-> (lib/query mp (lib.metadata/table mp (mt/id :checkins)))
        (lib/aggregate (lib/count))
        (lib/filter (lib/>= date-col "2026-05-01T00:00:00"))
        (lib/filter (lib/< date-col "2026-07-01T00:00:00")))))

(deftest neutral-occurrence-owner-preserves-subject-and-exact-fingerprint-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "r5-occurrence"
          query (string-date-query)]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id :query-id query-id
                        :query query :user-id owner-id})
        (let [occurrence (dima.occurrence/load-occurrence! convo-id query-id)]
          (is (= owner-id (:authenticated-subject occurrence)))
          (is (= (dima.occurrence/exact-query-fingerprint query)
                 (dima.occurrence/exact-query-fingerprint (:query occurrence)))))))))

(deftest neutral-occurrence-owner-fails-closed-on-foreign-missing-ambiguous-and-state-drift-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          other-id (mt/user->id :lucky)
          query (string-date-query)]
      (testing "foreign subject"
        (let [convo-id (str (random-uuid))]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id :query-id "q"
                            :query query :user-id owner-id}))
          (binding [api/*current-user-id* other-id]
            (is (= "NATIVE_OCCURRENCE_SUBJECT_MISMATCH"
                   (exception-code #(dima.occurrence/load-occurrence! convo-id "q")))))))
      (testing "missing occurrence"
        (let [convo-id (str (random-uuid))]
          (mt/with-current-user owner-id
            (metabot.persistence/start-turn!
             convo-id "internal" {:role "user" :content "r5"} :user-id owner-id)
            (is (= "NATIVE_QUERY_OCCURRENCE_NOT_FOUND"
                   (exception-code #(dima.occurrence/load-occurrence! convo-id "missing")))))))
      (testing "ambiguous occurrence"
        (let [convo-id (str (random-uuid))]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id :query-id "same"
                            :query query :user-id owner-id})
            (persist-turn! {:conversation-id convo-id :query-id "same"
                            :query query :user-id owner-id})
            (is (= "NATIVE_QUERY_OCCURRENCE_AMBIGUOUS"
                   (exception-code #(dima.occurrence/load-occurrence! convo-id "same")))))))
      (testing "producer/state mismatch"
        (let [convo-id (str (random-uuid))]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id :query-id "drift"
                            :query query :state-query (lib/limit query 2)
                            :user-id owner-id})
            (is (= "NATIVE_QUERY_STATE_MISMATCH"
                   (exception-code #(dima.occurrence/load-occurrence! convo-id "drift"))))))))))

(deftest r5-native-metric-identity-survives-multi-physical-expansion-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "metric-multi"
          definition (lib/aggregate
                      (lib/query (mt/metadata-provider)
                                 (lib.metadata/table (mt/metadata-provider) (mt/id :orders)))
                      (lib/count))]
      (mt/with-temp
        [:model/Card
         {metric-id :id metric-entity-id :entity_id}
         {:name "R5 Native Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [query (metric-query metric-id)]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id :query-id query-id
                            :query query :user-id owner-id})
            (let [out (observe! convo-id query-id)]
              (is (= [{:stage_number 0
                       :aggregation_index 0
                       :metabase_metric_id metric-id
                       :metabase_metric_entity_id metric-entity-id}]
                     (:native_metrics out)))
              (is (not (contains? out :aggregation_count)))
              (is (not (contains? out :aggregations))))))))))

(deftest r5-v3-temporal-fixture-is-metabase-produced-and-executable-test
  (mt/test-driver :h2
    (let [owner-id  (mt/user->id :rasta)
          source    (metabase-absolute-date-source-query)
          persisted (dima.occurrence/exact-serialized-query source)]
      (mt/with-current-user owner-id
        (let [result (qp/process-query
                      (qp/userland-query-with-default-constraints source))]
          (is (= :completed (:status result))))
        (is (= persisted
               (dima.occurrence/exact-serialized-query source)))))))

(deftest r5-temporal-observation-is-representation-independent-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          a-id (str (random-uuid))
          b-id (str (random-uuid))
          raw-source (string-date-query)]
      (mt/with-current-user owner-id
        (let [result (qp/process-query
                      (qp/userland-query-with-default-constraints raw-source))]
          (is (= :completed (:status result))))
        (persist-turn! {:conversation-id a-id :query-id "absolute"
                        :query (v3-persisted-absolute-date-query) :user-id owner-id})
        (persist-turn! {:conversation-id b-id :query-id "string"
                        :query (dima.occurrence/exact-serialized-query raw-source)
                        :user-id owner-id})
        (let [a (observe! a-id "absolute")
              b (observe! b-id "string")
              expected [{:time_field_id (mt/id :checkins :date)
                         :table_id (mt/id :checkins)
                         :lower_bound "2026-06-01"
                         :lower_inclusive true
                         :upper_bound "2026-07-01"
                         :upper_inclusive false}]]
          (is (= expected (:temporal_scopes a)))
          (is (= expected (:temporal_scopes b)))
          (is (= (:temporal_scopes a) (:temporal_scopes b))))))))

(deftest r8-paid-a-timestamp-literal-material-observation-equivalent-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "r8-paid-a-temporal-literal"
          query (dima.occurrence/exact-serialized-query
                 (r8-timestamp-string-query))]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id
                        :query-id query-id
                        :query query
                        :user-id owner-id})
        (let [out (observe! convo-id query-id)]
          (is (= "dima_native_material_observation_v1"
                 (:schema_version out)))
          (is (= [{:time_field_id (mt/id :checkins :date)
                   :table_id (mt/id :checkins)
                   :lower_bound "2026-05-01T00:00:00"
                   :lower_inclusive true
                   :upper_bound "2026-07-01T00:00:00"
                   :upper_inclusive false}]
                 (:temporal_scopes out))))))))

(deftest r5-observer-succeeds-while-p13-microscope-remains-strict-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "strict-microscope"
          query (v3-persisted-absolute-date-query)]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id :query-id query-id
                        :query query :user-id owner-id})
        (is (= "NATIVE_QUERY_RUNTIME_REPRESENTATION_UNSUPPORTED"
               (exception-code
                #(binding [dima.attestation/*runtime-identity-override* test-runtime]
                   (dima.attestation/attest-native-query!
                    {:conversation_id (java.util.UUID/fromString convo-id)
                     :native_query_id query-id})))))
        (is (= "dima_native_material_observation_v1"
               (:schema_version (observe! convo-id query-id))))))))

(deftest r5-ranking-and-breakout-are-observed-by-stable-native-identity-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "ranked"
          mp0 (mt/metadata-provider)
          definition (lib/aggregate
                      (lib/query mp0 (lib.metadata/table mp0 (mt/id :orders)))
                      (lib/count))]
      (mt/with-temp
        [:model/Card
         {metric-id :id metric-entity-id :entity_id}
         {:name "R5 Ranked Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [mp (mt/metadata-provider)
              base (metric-query metric-id)
              user-id-col (lib.metadata/field mp (mt/id :orders :user_id))
              ranked (-> base
                         (lib/breakout user-id-col)
                         (lib/order-by (lib/aggregation-ref base 0) :desc)
                         (lib/limit 5))]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id :query-id query-id
                            :query ranked :user-id owner-id})
            (let [out (observe! convo-id query-id)
                  rank (first (:ranking out))]
              (is (= "desc" (:direction rank)))
              (is (= 5 (:limit rank)))
              (is (= "level" (:basis rank)))
              (is (= "metric" (get-in rank [:target :kind])))
              (is (= metric-id (get-in rank [:target :metabase_metric_id])))
              (is (some #(and (= "breakout" (:role %))
                              (= (mt/id :orders :user_id) (:field_id %)))
                        (:dimensions out))))))))))

(deftest r5-change-ranking-observability-contract-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "change-ranking-observability"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          created-at (lib.metadata/field mp0 (mt/id :orders :created_at))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          definition (-> (lib/query mp0 orders)
                         (lib/aggregate (lib/sum total)))]
      (mt/with-temp
        [:model/Card
         {metric-id :id metric-entity-id :entity_id}
         {:name "R5 Change Ranking Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [mp (mt/metadata-provider)
              metric (lib.metadata/metric mp metric-id)
              date-col (lib.metadata/field mp (mt/id :orders :created_at))
              query0 (-> (lib/query mp (lib.metadata/table mp (mt/id :orders)))
                         (lib/breakout (lib/with-temporal-bucket date-col :month))
                         (lib/aggregate metric)
                         (lib/aggregate
                          (lib/- metric (lib/offset metric -1))))
              ranked (-> query0
                         (lib/order-by (lib/aggregation-ref query0 1) :desc)
                         (lib/limit 3))]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query ranked
                            :user-id owner-id})
            (let [out (observe! convo-id query-id)
                  rank (first (:ranking out))]
              ;; Independent observability law: a legal Metabase
              ;; period-over-period derived ranking must be distinguishable
              ;; from ordinary LEVEL ranking without executing the query.
              (is (= "change" (:basis rank)))
              (is (= "metric" (get-in rank [:target :kind])))
              (is (= metric-id
                     (get-in rank [:target :metabase_metric_id])))
              (is (= metric-entity-id
                     (get-in rank [:target :metabase_metric_entity_id]))))))))))

(deftest r5-percentage-change-ranking-observability-contract-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "percentage-change-ranking-observability"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          definition (-> (lib/query mp0 orders)
                         (lib/aggregate (lib/sum total)))]
      (mt/with-temp
        [:model/Card
         {metric-id :id metric-entity-id :entity_id}
         {:name "R5 Percentage Change Ranking Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [mp (mt/metadata-provider)
              metric (lib.metadata/metric mp metric-id)
              date-col (lib.metadata/field mp (mt/id :orders :created_at))
              change (lib/- (lib// metric (lib/offset metric -1)) 1.0)
              query0 (-> (lib/query mp (lib.metadata/table mp (mt/id :orders)))
                         (lib/breakout (lib/with-temporal-bucket date-col :month))
                         (lib/aggregate metric)
                         (lib/aggregate change))
              ranked (-> query0
                         (lib/order-by (lib/aggregation-ref query0 1) :desc)
                         (lib/limit 4))]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query ranked
                            :user-id owner-id})
            (let [rank (first (:ranking (observe! convo-id query-id)))]
              (is (= "change" (:basis rank)))
              (is (= "metric" (get-in rank [:target :kind])))
              (is (= metric-id
                     (get-in rank [:target :metabase_metric_id])))
              (is (= metric-entity-id
                     (get-in rank [:target :metabase_metric_entity_id]))))))))))

(deftest r5-change-ranking-without-leading-temporal-breakout-remains-unsupported-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "change-ranking-non-temporal-breakout"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          definition (-> (lib/query mp0 orders)
                         (lib/aggregate (lib/sum total)))]
      (mt/with-temp
        [:model/Card
         {metric-id :id metric-entity-id :entity_id}
         {:name "R5 Non Temporal Change Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [mp (mt/metadata-provider)
              metric (lib.metadata/metric mp metric-id)
              category (lib.metadata/field mp (mt/id :orders :product_id))
              query0 (-> (lib/query mp (lib.metadata/table mp (mt/id :orders)))
                         (lib/breakout category)
                         (lib/aggregate metric)
                         (lib/aggregate
                          (lib/- metric (lib/offset metric -1))))
              ranked (-> query0
                         (lib/order-by (lib/aggregation-ref query0 1) :desc)
                         (lib/limit 3))]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query ranked
                            :user-id owner-id})
            ;; Offset means previous row. Without a leading temporal breakout,
            ;; the observer cannot prove that this is period-over-period CHANGE.
            (is (= "NATIVE_MATERIAL_RANKING_TARGET_UNSUPPORTED"
                   (exception-code #(observe! convo-id query-id))))))))))

(deftest r5-change-ranking-with-temporal-breakout-not-first-remains-unsupported-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "change-ranking-temporal-breakout-not-first"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          definition (-> (lib/query mp0 orders)
                         (lib/aggregate (lib/sum total)))]
      (mt/with-temp
        [:model/Card
         {metric-id :id metric-entity-id :entity_id}
         {:name "R5 Ambiguous Change Ordering Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [mp (mt/metadata-provider)
              metric (lib.metadata/metric mp metric-id)
              category (lib.metadata/field mp (mt/id :orders :product_id))
              date-col (lib.metadata/field mp (mt/id :orders :created_at))
              query0 (-> (lib/query mp (lib.metadata/table mp (mt/id :orders)))
                         ;; Metabase Offset is row-relative. If time is not the
                         ;; leading breakout, period-over-period meaning is not
                         ;; structurally proven by the observer.
                         (lib/breakout category)
                         (lib/breakout (lib/with-temporal-bucket date-col :month))
                         (lib/aggregate metric)
                         (lib/aggregate
                          (lib/- metric (lib/offset metric -1))))
              ranked (-> query0
                         (lib/order-by (lib/aggregation-ref query0 1) :desc)
                         (lib/limit 3))]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query ranked
                            :user-id owner-id})
            (is (= "NATIVE_MATERIAL_RANKING_TARGET_UNSUPPORTED"
                   (exception-code #(observe! convo-id query-id))))))))))

(deftest r5-non-change-derived-ranking-remains-unsupported-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "non-change-derived-ranking"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          definition (-> (lib/query mp0 orders)
                         (lib/aggregate (lib/sum total)))]
      (mt/with-temp
        [:model/Card
         {metric-id :id}
         {:name "R5 Unsupported Derived Ranking Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [mp (mt/metadata-provider)
              metric (lib.metadata/metric mp metric-id)
              date-col (lib.metadata/field mp (mt/id :orders :created_at))
              not-change (lib/+ metric (lib/offset metric -1))
              query0 (-> (lib/query mp (lib.metadata/table mp (mt/id :orders)))
                         (lib/breakout (lib/with-temporal-bucket date-col :month))
                         (lib/aggregate metric)
                         (lib/aggregate not-change))
              ranked (-> query0
                         (lib/order-by (lib/aggregation-ref query0 1) :desc)
                         (lib/limit 3))]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query ranked
                            :user-id owner-id})
            (is (= "NATIVE_MATERIAL_RANKING_TARGET_UNSUPPORTED"
                   (exception-code #(observe! convo-id query-id))))))))))


(defn- previous-stage-column
  [query pred]
  (first (filter pred (lib/visible-columns query))))

(defn- period-pair-derived-query
  [metric-id combine-fn]
  (let [mp         (mt/metadata-provider)
        orders     (lib.metadata/table mp (mt/id :orders))
        entity     (lib.metadata/field mp (mt/id :orders :user_id))
        created-at (lib.metadata/field mp (mt/id :orders :created_at))
        metric     (lib.metadata/metric mp metric-id)
        stage0     (-> (lib/query mp orders)
                       (lib/breakout entity)
                       (lib/breakout created-at)
                       (lib/aggregate metric))
        stage1     (lib/append-stage stage0)
        visible1   (lib/visible-columns stage1)
        entity1    (previous-stage-column
                    stage1
                    #(= (mt/id :orders :user_id) (:id %)))
        date1      (previous-stage-column
                    stage1
                    #(= (mt/id :orders :created_at) (:id %)))
        metric1    (previous-stage-column
                    stage1
                    #(and (= :source/previous-stage (:lib/source %))
                          (nil? (:id %))
                          (not (:lib/breakout? %))))
        may-start        "2026-05-01T00:00:00"
        baseline-end     "2026-06-01T00:00:00"
        comparison-start "2026-06-01T00:00:00"
        july-start       "2026-07-01T00:00:00"
        baseline   (lib/with-expression-name
                    (lib/sum-where
                     metric1
                     (lib/and (lib/>= date1 may-start)
                              (lib/< date1 baseline-end)))
                    "Baseline Total")
        comparison (lib/with-expression-name
                    (lib/sum-where
                     metric1
                     (lib/and (lib/>= date1 comparison-start)
                              (lib/< date1 july-start)))
                    "Comparison Total")
        stage1a    (-> stage1
                       (lib/aggregate baseline)
                       (lib/aggregate comparison)
                       (lib/breakout entity1))
        stage2     (lib/append-stage stage1a)
        baseline2  (previous-stage-column
                    stage2
                    #(= "Baseline Total" (:display-name %)))
        compare2   (previous-stage-column
                    stage2
                    #(= "Comparison Total" (:display-name %)))
        delta-name "Period Delta"
        stage2a    (lib/expression stage2 delta-name (combine-fn compare2 baseline2))]
    (lib/order-by stage2a (lib/expression-ref stage2a delta-name) :desc)))

(deftest r5-period-pair-derived-change-ranking-observability-contract-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "period-pair-derived-change-ranking"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          definition (-> (lib/query mp0 orders)
                         (lib/aggregate (lib/sum total)))]
      (mt/with-temp
        [:model/Card
         {metric-id :id metric-entity-id :entity_id}
         {:name "R5 Period Pair Change Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [query (period-pair-derived-query metric-id lib/-)]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query query
                            :user-id owner-id})
            ;; Legal alternative CHANGE material: two disjoint period
            ;; aggregations over one governed metric, followed by comparison -
            ;; baseline and ordering by that derived value. The observer must
            ;; recover the stable governed metric identity structurally.
            (let [out (observe! convo-id query-id)
                  rank (last (:ranking out))]
              (is (= "change" (:basis rank)))
              (is (= "metric" (get-in rank [:target :kind])))
              (is (= metric-id
                     (get-in rank [:target :metabase_metric_id])))
              (is (= metric-entity-id
                     (get-in rank [:target :metabase_metric_entity_id])))
              (is (= "desc" (:direction rank))))))))))



(deftest r5-period-pair-non-change-derived-ranking-remains-unsupported-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "period-pair-non-change-ranking"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          definition (-> (lib/query mp0 orders)
                         (lib/aggregate (lib/sum total)))]
      (mt/with-temp
        [:model/Card
         {metric-id :id}
         {:name "R5 Period Pair Non Change Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [query (period-pair-derived-query metric-id lib/+)]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query query
                            :user-id owner-id})
            (is (= "NATIVE_MATERIAL_RANKING_TARGET_UNSUPPORTED"
                   (exception-code #(observe! convo-id query-id))))))))))



(deftest r5-period-pair-reversed-temporal-delta-remains-unsupported-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "period-pair-reversed-delta"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          definition (-> (lib/query mp0 orders)
                         (lib/aggregate (lib/sum total)))]
      (mt/with-temp
        [:model/Card
         {metric-id :id}
         {:name "R5 Reversed Period Pair Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [query (period-pair-derived-query
                     metric-id
                     (fn [comparison baseline]
                       (lib/- baseline comparison)))]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query query
                            :user-id owner-id})
            ;; The arithmetic direction is semantically material. Earlier -
            ;; later must not be certified as comparison-minus-baseline CHANGE.
            (is (= "NATIVE_MATERIAL_RANKING_TARGET_UNSUPPORTED"
                   (exception-code #(observe! convo-id query-id))))))))))

(defn- mixed-metric-period-pair-query
  [baseline-metric-id comparison-metric-id]
  (let [mp         (mt/metadata-provider)
        orders     (lib.metadata/table mp (mt/id :orders))
        entity     (lib.metadata/field mp (mt/id :orders :user_id))
        created-at (lib.metadata/field mp (mt/id :orders :created_at))
        metric-a   (lib.metadata/metric mp baseline-metric-id)
        metric-b   (lib.metadata/metric mp comparison-metric-id)
        stage0     (-> (lib/query mp orders)
                       (lib/breakout entity)
                       (lib/breakout created-at)
                       (lib/aggregate metric-a)
                       (lib/aggregate metric-b))
        stage1     (lib/append-stage stage0)
        visible1   (lib/visible-columns stage1)
        entity1    (previous-stage-column
                    stage1
                    #(= (mt/id :orders :user_id) (:id %)))
        date1      (previous-stage-column
                    stage1
                    #(= (mt/id :orders :created_at) (:id %)))
        metric-cols (vec
                     (filter
                      #(and (= :source/previous-stage (:lib/source %))
                            (nil? (:id %))
                            (not (:lib/breakout? %)))
                      visible1))
        baseline   (lib/with-expression-name
                    (lib/sum-where
                     (first metric-cols)
                     (lib/and
                      (lib/>= date1 "2026-05-01T00:00:00")
                      (lib/< date1 "2026-06-01T00:00:00")))
                    "Baseline Total")
        comparison (lib/with-expression-name
                    (lib/sum-where
                     (second metric-cols)
                     (lib/and
                      (lib/>= date1 "2026-06-01T00:00:00")
                      (lib/< date1 "2026-07-01T00:00:00")))
                    "Comparison Total")
        stage1a    (-> stage1
                       (lib/aggregate baseline)
                       (lib/aggregate comparison)
                       (lib/breakout entity1))
        stage2     (lib/append-stage stage1a)
        baseline2  (previous-stage-column
                    stage2
                    #(= "Baseline Total" (:display-name %)))
        compare2   (previous-stage-column
                    stage2
                    #(= "Comparison Total" (:display-name %)))
        delta-name "Mixed Metric Delta"
        stage2a    (lib/expression stage2 delta-name (lib/- compare2 baseline2))]
    (lib/order-by stage2a (lib/expression-ref stage2a delta-name) :desc)))

(deftest r5-period-pair-mixed-governed-metrics-remain-unsupported-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "period-pair-mixed-metrics"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          quantity (lib.metadata/field mp0 (mt/id :orders :quantity))
          def-a (-> (lib/query mp0 orders)
                    (lib/aggregate (lib/sum total)))
          def-b (-> (lib/query mp0 orders)
                    (lib/aggregate (lib/sum quantity)))]
      (mt/with-temp
        [:model/Card
         {metric-a-id :id}
         {:name "R5 Period Pair Metric A"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query def-a}
         :model/Card
         {metric-b-id :id}
         {:name "R5 Period Pair Metric B"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query def-b}]
        (let [query (mixed-metric-period-pair-query metric-a-id metric-b-id)]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query query
                            :user-id owner-id})
            (is (= "NATIVE_MATERIAL_RANKING_TARGET_UNSUPPORTED"
                   (exception-code #(observe! convo-id query-id))))))))))


(deftest r5-production-observer-has-executable-zero-p13-and-zero-execution-dependency-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "architecture-guard"
          query (dima.occurrence/exact-serialized-query (string-date-query))
          p13-calls (atom 0)
          process-calls (atom 0)]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id :query-id query-id
                        :query query :user-id owner-id})
        (with-redefs [dima.attestation/attest-native-query!
                      (fn [& _]
                        (swap! p13-calls inc)
                        (throw (ex-info "P13 must not be called" {})))
                      qp/process-query
                      (fn [& _]
                        (swap! process-calls inc)
                        (throw (ex-info "observer must not execute analytics" {})))]
          (is (= "dima_native_material_observation_v1"
                 (:schema_version (observe! convo-id query-id)))))
        (is (zero? @p13-calls))
        (is (zero? @process-calls))
        (let [deps (->> (ns-aliases 'metabase.dima.native-material-observation)
                        vals
                        (map ns-name)
                        set)]
          (is (not (contains? deps 'metabase.dima.native-attestation)))
          (is (not (contains? deps 'metabase.query-processor)))
          (is (not (contains? deps 'metabase.query-processor.api))))))))

(deftest r5-observer-does-not-rewrite-exact-occurrence-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "immutable"
          query (v3-persisted-absolute-date-query)
          before (dima.occurrence/exact-query-fingerprint query)]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id :query-id query-id
                        :query query :user-id owner-id})
        (observe! convo-id query-id)
        (let [after (dima.occurrence/load-occurrence! convo-id query-id)]
          (is (= before (dima.occurrence/exact-query-fingerprint (:query after)))))))))


(defn- period-pair-equality-derived-query
  ([metric-id]
   (period-pair-equality-derived-query
    metric-id
    "2026-05-01T00:00:00"
    "2026-06-01T00:00:00"
    true))
  ([metric-id baseline-value comparison-value bucketed?]
   (let [mp         (mt/metadata-provider)
         orders     (lib.metadata/table mp (mt/id :orders))
         entity     (lib.metadata/field mp (mt/id :orders :user_id))
         created-at (lib.metadata/field mp (mt/id :orders :created_at))
         time-ref   (if bucketed?
                      (lib/with-temporal-bucket created-at :month)
                      created-at)
         metric     (lib.metadata/metric mp metric-id)
         stage0     (-> (lib/query mp orders)
                        (lib/breakout entity)
                        (lib/breakout time-ref)
                        (lib/aggregate metric))
        stage1     (lib/append-stage stage0)
        entity1    (previous-stage-column
                    stage1
                    #(= (mt/id :orders :user_id) (:id %)))
        date1      (previous-stage-column
                    stage1
                    #(= (mt/id :orders :created_at) (:id %)))
        metric1    (previous-stage-column
                    stage1
                    #(and (= :source/previous-stage (:lib/source %))
                          (nil? (:id %))
                          (not (:lib/breakout? %))))
        baseline   (lib/with-expression-name
                    (lib/sum-where
                     metric1
                     (lib.filter/filter-clause := date1 baseline-value))
                    "Baseline Total")
        comparison (lib/with-expression-name
                    (lib/sum-where
                     metric1
                     (lib.filter/filter-clause := date1 comparison-value))
                    "Comparison Total")
        stage1a    (-> stage1
                       (lib/aggregate baseline)
                       (lib/aggregate comparison)
                       (lib/breakout entity1))
        stage2     (lib/append-stage stage1a)
        baseline2  (previous-stage-column
                    stage2
                    #(= "Baseline Total" (:display-name %)))
        compare2   (previous-stage-column
                    stage2
                    #(= "Comparison Total" (:display-name %)))
        delta-name "Period Delta"
        stage2a    (lib/expression stage2 delta-name (lib/- compare2 baseline2))]
     (lib/order-by stage2a (lib/expression-ref stage2a delta-name) :desc))))

(deftest r5-live-shape-month-bucket-equality-period-pair-change-reproducer-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "live-shape-month-bucket-equality-period-pair"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          definition (-> (lib/query mp0 orders)
                         (lib/aggregate (lib/sum total)))]
      (mt/with-temp
        [:model/Card
         {metric-id :id metric-entity-id :entity_id}
         {:name "R5 Equality Period Pair Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [query (period-pair-equality-derived-query metric-id)]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query query
                            :user-id owner-id})
            ;; Exact live structural family: month-bucketed previous-stage date,
            ;; one equality-selected aggregate per period, comparison - baseline,
            ;; then DESC ordering by the derived expression.
            ;; Certified dima.11 should classify this legal material as CHANGE.
            (let [out (observe! convo-id query-id)
                  rank (last (:ranking out))]
              (is (= "change" (:basis rank)))
              (is (= "metric" (get-in rank [:target :kind])))
              (is (= metric-id
                     (get-in rank [:target :metabase_metric_id])))
              (is (= metric-entity-id
                     (get-in rank [:target :metabase_metric_entity_id])))
              (is (= "desc" (:direction rank))))))))))





(defn- live-unnamed-equality-period-pair-query
  [metric-id]
  (let [mp         (mt/metadata-provider)
        orders     (lib.metadata/table mp (mt/id :orders))
        entity     (lib.metadata/field mp (mt/id :orders :user_id))
        created-at (lib.metadata/field mp (mt/id :orders :created_at))
        month-ref  (lib/with-temporal-bucket created-at :month)
        metric     (lib.metadata/metric mp metric-id)
        stage0     (-> (lib/query mp orders)
                       (lib/breakout entity)
                       (lib/breakout month-ref)
                       (lib/aggregate metric)
                       (lib/filter (lib/>= created-at "2026-05-01"))
                       (lib/filter (lib/< created-at "2026-07-01")))
        stage1     (lib/append-stage stage0)
        visible1   (lib/visible-columns stage1)
        entity1    (previous-stage-column
                    stage1
                    #(= (mt/id :orders :user_id) (:id %)))
        date1      (previous-stage-column
                    stage1
                    #(= (mt/id :orders :created_at) (:id %)))
        metric1    (previous-stage-column
                    stage1
                    #(and (= :source/previous-stage (:lib/source %))
                          (nil? (:id %))
                          (not (:lib/breakout? %))))
        baseline   (lib/sum-where
                    metric1
                    (lib.filter/filter-clause := date1 "2026-05-01"))
        comparison (lib/sum-where
                    metric1
                    (lib.filter/filter-clause := date1 "2026-06-01"))
        stage1a    (-> stage1
                       (lib/aggregate baseline)
                       (lib/aggregate comparison)
                       (lib/breakout entity1))
        stage2     (lib/append-stage stage1a)
        aggregate-cols
        (vec
         (filter
          #(and (= :source/previous-stage (:lib/source %))
                (nil? (:id %))
                (not (:lib/breakout? %)))
          (lib/visible-columns stage2)))
        baseline2  (first aggregate-cols)
        comparison2 (second aggregate-cols)
        delta-name "Live Unnamed Delta"
        stage2a    (lib/expression
                    stage2
                    delta-name
                    (lib/- comparison2 baseline2))]
    (lib/order-by stage2a (lib/expression-ref stage2a delta-name) :desc)))

(deftest r5-live-unnamed-equality-period-pair-change-reproducer-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "live-unnamed-equality-period-pair"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          definition (-> (lib/query mp0 orders)
                         (lib/aggregate (lib/sum total)))]
      (mt/with-temp
        [:model/Card
         {metric-id :id metric-entity-id :entity_id}
         {:name "R5 Live Unnamed Equality Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [query (live-unnamed-equality-period-pair-query metric-id)
              order-by (first (lib/order-bys query 2))
              target (nth order-by 2 nil)
              resolved (if (and (vector? target)
                                (= :expression (first target)))
                         (lib.expression/resolve-expression query 2 (last target))
                         target)
              parts (lib/expression-parts query 2 resolved)
              _ (println
                 "DIMA11P1_UNNAMED_EXPR_TRACE"
                 (pr-str
                  {:order-by order-by
                   :target target
                   :resolved resolved
                   :parts parts
                   :args (mapv #(if (map? %)
                                  (select-keys %
                                               [:display-name
                                                :name
                                                :lib/source
                                                :lib/source-uuid
                                                :lib/source-column-alias
                                                :lib/desired-column-alias
                                                :id])
                                  %)
                               (:args parts))}))]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query query
                            :user-id owner-id})
            (let [out (observe! convo-id query-id)
                  rank (last (:ranking out))]
              (is (= "change" (:basis rank)))
              (is (= "metric" (get-in rank [:target :kind])))
              (is (= metric-id
                     (get-in rank [:target :metabase_metric_id])))
              (is (= metric-entity-id
                     (get-in rank [:target :metabase_metric_entity_id])))
              (is (= "desc" (:direction rank))))))))))

(deftest r5-equality-period-pair-same-bucket-remains-unsupported-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "equality-period-pair-same-bucket"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          definition (-> (lib/query mp0 orders)
                         (lib/aggregate (lib/sum total)))]
      (mt/with-temp
        [:model/Card
         {metric-id :id}
         {:name "R5 Equality Same Bucket Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [query (period-pair-equality-derived-query
                     metric-id
                     "2026-05-01T00:00:00"
                     "2026-05-01T00:00:00"
                     true)]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query query
                            :user-id owner-id})
            (is (= "NATIVE_MATERIAL_RANKING_TARGET_UNSUPPORTED"
                   (exception-code #(observe! convo-id query-id))))))))))


(deftest r5-unbucketed-temporal-equality-period-pair-remains-unsupported-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "unbucketed-equality-period-pair"
          mp0 (mt/metadata-provider)
          orders (lib.metadata/table mp0 (mt/id :orders))
          total (lib.metadata/field mp0 (mt/id :orders :total))
          definition (-> (lib/query mp0 orders)
                         (lib/aggregate (lib/sum total)))]
      (mt/with-temp
        [:model/Card
         {metric-id :id}
         {:name "R5 Equality Unbucketed Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [query (period-pair-equality-derived-query
                     metric-id
                     "2026-05-01T00:00:00"
                     "2026-06-01T00:00:00"
                     false)]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query query
                            :user-id owner-id})
            (is (= "NATIVE_MATERIAL_RANKING_TARGET_UNSUPPORTED"
                   (exception-code #(observe! convo-id query-id))))))))))
