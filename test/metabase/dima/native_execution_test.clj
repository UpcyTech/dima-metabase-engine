(ns metabase.dima.native-execution-test
  (:require
   [clojure.data :as data]
   [clojure.test :refer :all]
   [clojure.walk :as walk]
   [metabase.api.common :as api]
   [metabase.dima.native-attestation :as dima.attestation]
   [metabase.dima.native-execution :as dima.execution]
   [metabase.dima.native-query-compat :as dima.compat]
   [metabase.lib-be.core :as lib-be]
   [metabase.lib.core :as lib]
   [metabase.lib.normalize :as lib.normalize]
   [metabase.lib.schema :as lib.schema]
   [metabase.lib.serialize :as lib.serialize]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.metabot.persistence :as metabot.persistence]
   [metabase.query-processor :as qp]
   [metabase.test :as mt]
   [metabase.test.fixtures :as fixtures]
   [toucan2.core :as t2]))

(use-fixtures :once (fixtures/initialize :db))

(def ^:private test-runtime
  {:repository          "UpcyTech/dima-metabase-engine"
   :revision_sha        "2222222222222222222222222222222222222222"
   :upstream_base_sha   "2ba2485c78d7e00a9a25f82c00fc201da71590c4"
   :runtime_tag         "v0.63.18-dima.6-test"
   :build_identity      "test-build:dima6"
   :image_identity      "local-image:dima6"
   :runtime_instance_id "00000000-0000-4000-8000-000000000016"})

(defn- exception-code [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      (:dima/error-code (ex-data e)))))

(defn- count-query []
  (let [mp (mt/metadata-provider)]
    (lib/aggregate
     (lib/query mp (lib.metadata/table mp (mt/id :orders)))
     (lib/count))))

(defn- persisted-absolute-ranking-query []
  (let [mp         (mt/metadata-provider)
        created-at (lib.metadata/field mp (mt/id :orders :created_at))
        product-id (lib.metadata/field mp (mt/id :orders :product_id))
        lower      (lib/absolute-datetime
                    (java.time.LocalDateTime/parse "2010-01-01T00:00:00")
                    :day)
        upper      (lib/absolute-datetime
                    (java.time.LocalDateTime/parse "2030-12-31T23:59:59")
                    :day)
        query      (-> (count-query)
                       (lib/filter (lib/between created-at lower upper))
                       (lib/breakout product-id))]
    (-> query
        (lib/order-by (lib/aggregation-ref query 0) :desc)
        (lib/limit 2))))

(defn- persist-turn!
  [{:keys [conversation-id query-id query user-id state-query]}]
  (let [{:keys [assistant-msg-id]}
        (metabot.persistence/start-turn!
         conversation-id
         "internal"
         {:role "user" :content "dima6 exact occurrence test"}
         :user-id user-id)]
    (metabot.persistence/finalize-assistant-turn!
     conversation-id
     assistant-msg-id
     [{:type :tool-input
       :id "call-dima6"
       :function "construct_notebook_query"
       :arguments {}}
      {:type :tool-output
       :id "call-dima6"
       :result {:output "ok"
                :structured-output {:query-id query-id
                                    :query query}}}
      {:type :data
       :data-type "state"
       :data {:queries {query-id (or state-query query)}}}])
    assistant-msg-id))

(defn- attest!
  [conversation-id query-id]
  (binding [dima.attestation/*runtime-identity-override* test-runtime]
    (dima.attestation/attest-native-query!
     {:conversation_id (java.util.UUID/fromString conversation-id)
      :native_query_id query-id})))

(defn- execute!
  [conversation-id query-id attestation]
  (binding [dima.attestation/*runtime-identity-override* test-runtime]
    (dima.execution/execute-native-query!
     {:conversation_id (java.util.UUID/fromString conversation-id)
      :native_query_id query-id
      :expected_pmbql_fingerprint (get-in attestation [:manifest :exact_pmbql_fingerprint])
      :expected_attestation_id (get-in attestation [:manifest :attestation_id])})))

(deftest compatibility-hydration-is-narrow-and-reversible-test
  (mt/test-driver :h2
    (let [query      (persisted-absolute-ranking-query)
          exact      (dima.compat/exact-serialized-query query)
          database-id (get exact "database")
          internal   (-> (lib.normalize/normalize
                          ::lib.schema/query
                          exact
                          {:throw? true})
                         lib.serialize/prepare-after-deserialization)
          hydrated*  (dima.compat/hydrate-exact-serialized-query! internal)
          runtime*   (assoc hydrated*
                            :lib/metadata
                            (lib-be/application-database-metadata-provider database-id))
          roundtrip* (dima.compat/exact-serialized-query runtime*)]
      (is (= exact roundtrip*)
          (pr-str (data/diff exact roundtrip*)))
      (let [hydrated (dima.compat/restore-exact-query! exact)]
        (is (= exact (dima.compat/exact-serialized-query hydrated)))
        (is (= (dima.attestation/exact-query-fingerprint query)
               (dima.attestation/exact-query-fingerprint hydrated)))))))

(deftest unsupported-absolute-datetime-representation-fails-closed-test
  (mt/test-driver :h2
    (let [exact (dima.compat/exact-serialized-query (persisted-absolute-ranking-query))
          changed? (atom false)
          bad (walk/postwalk
               (fn [value]
                 (if (and (not @changed?)
                          (vector? value)
                          (contains? #{"absolute-datetime" :absolute-datetime} (first value)))
                   (do
                     (reset! changed? true)
                     (assoc value 2 "June 2026"))
                   value))
               exact)]
      (is @changed?)
      (is (= "NATIVE_QUERY_COMPAT_UNSUPPORTED_TEMPORAL_REPRESENTATION"
             (exception-code #(dima.compat/restore-exact-query! bad)))))))

(deftest exact-occurrence-attestation-and-execution-match-native-query-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "dima6-temporal-ranking"
          query (persisted-absolute-ranking-query)]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id
                        :query-id query-id
                        :query query
                        :user-id owner-id})
        (binding [dima.attestation/*runtime-identity-override* test-runtime]
          (let [native-result (qp/process-query
                              (qp/userland-query-with-default-constraints
                               query
                               {:executed-by owner-id :context :ad-hoc}))
                attestation   (attest! convo-id query-id)
                execution     (execute! convo-id query-id attestation)]
            (is (= :completed (:status native-result)))
            (is (= 2 (:row_count native-result)))
            (is (= (:data native-result) (get-in execution [:result :data])))
            (is (= (:row_count native-result) (get-in execution [:result :row_count])))
            (is (= (get-in attestation [:manifest :exact_pmbql_fingerprint])
                   (:executed_exact_pmbql_fingerprint execution)))
            (is (= (get-in attestation [:manifest :attestation_id])
                   (:attestation_id execution)))))))))

(deftest wrong-expected-fingerprint-blocks-before-query-execution-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "dima6-wrong-fingerprint"
          query (persisted-absolute-ranking-query)]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id :query-id query-id :query query :user-id owner-id})
        (let [attestation (attest! convo-id query-id)]
          (with-redefs [qp/process-query
                        (fn [& _]
                          (throw (ex-info "DB execution must not happen" {:dima/error-code "EXECUTED"})))]
            (is (= "NATIVE_EXECUTION_FINGERPRINT_MISMATCH"
                   (exception-code
                    #(binding [dima.attestation/*runtime-identity-override* test-runtime]
                       (dima.execution/execute-native-query!
                        {:conversation_id (java.util.UUID/fromString convo-id)
                         :native_query_id query-id
                         :expected_pmbql_fingerprint (apply str (repeat 64 "f"))
                         :expected_attestation_id (get-in attestation [:manifest :attestation_id])})))))))))))

(deftest wrong-current-subject-blocks-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          other-id (mt/user->id :lucky)
          convo-id (str (random-uuid))
          query-id "dima6-wrong-subject"
          query (persisted-absolute-ranking-query)]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id :query-id query-id :query query :user-id owner-id})
        (let [attestation (attest! convo-id query-id)]
          (binding [api/*current-user-id* other-id
                    dima.attestation/*runtime-identity-override* test-runtime]
            (is (= "NATIVE_ATTESTATION_SUBJECT_MISMATCH"
                   (exception-code
                    #(dima.execution/execute-native-query!
                      {:conversation_id (java.util.UUID/fromString convo-id)
                       :native_query_id query-id
                       :expected_pmbql_fingerprint (get-in attestation [:manifest :exact_pmbql_fingerprint])
                       :expected_attestation_id (get-in attestation [:manifest :attestation_id])}))))))))))

(deftest producer-state-mismatch-blocks-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "dima6-state-mismatch"
          query (persisted-absolute-ranking-query)]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id
                        :query-id query-id
                        :query query
                        :state-query (count-query)
                        :user-id owner-id})
        (binding [dima.attestation/*runtime-identity-override* test-runtime]
          (is (= "NATIVE_QUERY_STATE_MISMATCH"
                 (exception-code
                  #(dima.execution/execute-native-query!
                    {:conversation_id (java.util.UUID/fromString convo-id)
                     :native_query_id query-id
                     :expected_pmbql_fingerprint (apply str (repeat 64 "a"))
                     :expected_attestation_id "dima_att_expected"})))))))))

(deftest mutation-after-attestation-blocks-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "dima6-post-attestation-mutation"
          query (persisted-absolute-ranking-query)]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id :query-id query-id :query query :user-id owner-id})
        (let [attestation (attest! convo-id query-id)]
          (t2/update! :model/MetabotConversation
                      :id convo-id
                      {:state {:queries {query-id (count-query)}}})
          (binding [dima.attestation/*runtime-identity-override* test-runtime]
            (is (= "NATIVE_QUERY_STATE_MISMATCH"
                   (exception-code
                    #(dima.execution/execute-native-query!
                      {:conversation_id (java.util.UUID/fromString convo-id)
                       :native_query_id query-id
                       :expected_pmbql_fingerprint (get-in attestation [:manifest :exact_pmbql_fingerprint])
                       :expected_attestation_id (get-in attestation [:manifest :attestation_id])}))))))))))

(deftest execution-endpoint-rejects-caller-supplied-query-body-test
  (is (= "The request contains keys or values this endpoint does not accept."
         (mt/user-http-request
          :rasta
          :post
          400
          "dima/engine/v1/native-query-execution"
          {:conversation_id (str (random-uuid))
           :native_query_id "q"
           :expected_pmbql_fingerprint (apply str (repeat 64 "a"))
           :expected_attestation_id "dima_att_expected"
           :exact_serialized_pmbql {}}))))
