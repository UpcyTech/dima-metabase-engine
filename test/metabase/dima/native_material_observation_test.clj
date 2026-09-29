(ns metabase.dima.native-material-observation-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [metabase.api.common :as api]
   [metabase.dima.native-attestation :as dima.attestation]
   [metabase.dima.native-material-observation :as dima.material]
   [metabase.dima.native-occurrence :as dima.occurrence]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.metabot.persistence :as metabot.persistence]
   [metabase.test :as mt]
   [metabase.test.fixtures :as fixtures]))

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

(defn- absolute-date-query []
  (let [mp (mt/metadata-provider)
        date-col (lib.metadata/field mp (mt/id :checkins :date))
        lower [:absolute-datetime
               {:lib/uuid "00000000-0000-4000-8000-000000000091"
                :base-type :type/Date}
               "2026-06-01"
               :day]
        upper [:absolute-datetime
               {:lib/uuid "00000000-0000-4000-8000-000000000092"
                :base-type :type/Date}
               "2026-07-01"
               :day]]
    (-> (lib/query mp (lib.metadata/table mp (mt/id :checkins)))
        (lib/aggregate (lib/count))
        (lib/filter (lib/>= date-col lower))
        (lib/filter (lib/< date-col upper)))))

(defn- string-date-query []
  (let [mp (mt/metadata-provider)
        date-col (lib.metadata/field mp (mt/id :checkins :date))]
    (-> (lib/query mp (lib.metadata/table mp (mt/id :checkins)))
        (lib/aggregate (lib/count))
        (lib/filter (lib/>= date-col "2026-06-01"))
        (lib/filter (lib/< date-col "2026-07-01")))))

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

(deftest r5-temporal-observation-is-representation-independent-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          a-id (str (random-uuid))
          b-id (str (random-uuid))]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id a-id :query-id "absolute"
                        :query (absolute-date-query) :user-id owner-id})
        (persist-turn! {:conversation-id b-id :query-id "string"
                        :query (string-date-query) :user-id owner-id})
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

(deftest r5-observer-succeeds-while-p13-microscope-remains-strict-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "strict-microscope"
          query (absolute-date-query)]
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
              (is (= "metric" (get-in rank [:target :kind])))
              (is (= metric-id (get-in rank [:target :metabase_metric_id])))
              (is (some #(and (= "breakout" (:role %))
                              (= (mt/id :orders :user_id) (:field_id %)))
                        (:dimensions out))))))))))

(deftest r5-production-observer-has-no-p13-dataset-or-representation-grammar-dependency-test
  (let [source (slurp "src/metabase/dima/native_material_observation.clj")]
    (doseq [forbidden ["native-attestation"
                       "attest-native-query"
                       "execute-dataset"
                       "/api/dataset"
                       "process-query"
                       "LocalDate"
                       "LocalDateTime"
                       "OffsetDateTime"
                       "ZonedDateTime"
                       "absolute-datetime"
                       "sqlparse"
                       "benchmark"
                       "candidate_id"
                       "semantic_id"]]
      (is (not (str/includes? source forbidden)) forbidden))))

(deftest r5-observer-does-not-rewrite-exact-occurrence-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "immutable"
          query (absolute-date-query)
          before (dima.occurrence/exact-query-fingerprint query)]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id :query-id query-id
                        :query query :user-id owner-id})
        (observe! convo-id query-id)
        (let [after (dima.occurrence/load-occurrence! convo-id query-id)]
          (is (= before (dima.occurrence/exact-query-fingerprint (:query after)))))))))
