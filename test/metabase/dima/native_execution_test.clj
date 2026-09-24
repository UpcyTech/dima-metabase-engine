(ns metabase.dima.native-execution-test
  (:require
   [clojure.test :refer :all]
   [metabase.api.common :as api]
   [metabase.dima.native-attestation :as dima.attestation]
   [metabase.dima.native-execution :as dima.execution]
   [metabase.dima.native-query-compat :as dima.compat]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.metabot.persistence :as metabot.persistence]
   [metabase.query-processor :as qp]
   [metabase.test :as mt]
   [metabase.test.fixtures :as fixtures]
   [toucan2.core :as t2])
  (:import
   (java.time LocalDateTime OffsetDateTime)
   (java.time.temporal Temporal)))

(use-fixtures :once (fixtures/initialize :db))

(def ^:private test-runtime
  {:repository          "UpcyTech/dima-metabase-engine"
   :revision_sha        "6666666666666666666666666666666666666666"
   :upstream_base_sha   "2ba2485c78d7e00a9a25f82c00fc201da71590c4"
   :runtime_tag         "v0.63.18-dima.6-test"
   :build_identity      "test-build:p13d-exact-occurrence"
   :image_identity      "local-image:p13d-exact-occurrence"
   :runtime_instance_id "00000000-0000-4000-8000-000000000066"})

(defn- exception-code [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      (:dima/error-code (ex-data e)))))

(defn- count-star-query []
  (let [mp    (mt/metadata-provider)
        table (lib.metadata/table mp (mt/id :orders))]
    (-> (lib/query mp table)
        (lib/aggregate (lib/count)))))

(defn- temporal-ranking-query []
  (let [mp         (mt/metadata-provider)
        table      (lib.metadata/table mp (mt/id :orders))
        created-at (lib.metadata/field mp (mt/id :orders :created_at))
        product-id (lib.metadata/field mp (mt/id :orders :product_id))
        lower      (lib/absolute-datetime
                    (LocalDateTime/parse "2010-01-01T00:00")
                    :day)
        upper      (lib/absolute-datetime
                    (LocalDateTime/parse "2030-01-01T00:00")
                    :day)
        query      (-> (lib/query mp table)
                       (lib/aggregate (lib/count))
                       (lib/filter (lib/between created-at lower upper))
                       (lib/breakout product-id))]
    (-> query
        (lib/order-by (lib/aggregation-ref query 0) :desc)
        (lib/limit 2))))

(defn- offset-temporal-query []
  (let [mp         (mt/metadata-provider)
        table      (lib.metadata/table mp (mt/id :orders))
        created-at (lib.metadata/field mp (mt/id :orders :created_at))
        lower      (lib/absolute-datetime
                    (OffsetDateTime/parse "2010-01-01T00:00:00+03:00")
                    :day)]
    (-> (lib/query mp table)
        (lib/aggregate (lib/count))
        (lib/filter (lib/>= created-at lower)))))

(defn- tool-parts [query-id query]
  [{:type :tool-input
    :id "call-1"
    :function "construct_notebook_query"
    :arguments {}}
   {:type :tool-output
    :id "call-1"
    :result {:output "ok"
             :structured-output {:query-id query-id
                                 :query query}}}])

(defn- persist-turn!
  [{:keys [conversation-id query-id query user-id state-query]}]
  (let [{:keys [assistant-msg-id]}
        (metabot.persistence/start-turn!
         conversation-id
         "internal"
         {:role "user" :content "test"}
         :user-id user-id)]
    (metabot.persistence/finalize-assistant-turn!
     conversation-id
     assistant-msg-id
     (conj (vec (tool-parts query-id query))
           {:type :data
            :data-type "state"
            :data {:queries {query-id (or state-query query)}}}))
    assistant-msg-id))

(defn- attest! [conversation-id query-id]
  (binding [dima.attestation/*runtime-identity-override* test-runtime]
    (dima.attestation/attest-native-query!
     {:conversation_id (java.util.UUID/fromString conversation-id)
      :native_query_id query-id})))

(defn- execute! [conversation-id query-id attestation]
  (binding [dima.attestation/*runtime-identity-override* test-runtime]
    (dima.execution/execute-native-query!
     {:conversation_id (java.util.UUID/fromString conversation-id)
      :native_query_id query-id
      :expected_pmbql_fingerprint
      (get-in attestation [:manifest :exact_pmbql_fingerprint])
      :expected_attestation_id
      (get-in attestation [:manifest :attestation_id])})))

(deftest absolute-datetime-compatibility-hydration-is-lossless-test
  (mt/test-driver :h2
    (let [query     (temporal-ranking-query)
          authority (dima.attestation/exact-serialized-query query)
          restored  (dima.compat/restore-exact-runtime-query! authority)
          temporal-values
          (->> (tree-seq coll? seq restored)
               (filter #(and (vector? %)
                             (= :absolute-datetime (first %))))
               (map #(nth % 2))
               vec)]
      (is (= authority (dima.attestation/exact-serialized-query restored)))
      (is (= 2 (count temporal-values)))
      (is (every? #(instance? Temporal %) temporal-values)))))

(deftest persisted-temporal-ranking-attests-and-executes-the-same-occurrence-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "p13d-temporal-ranking"]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id
                        :query-id query-id
                        :query (temporal-ranking-query)
                        :user-id owner-id})
        (let [attestation (attest! convo-id query-id)
              execution   (execute! convo-id query-id attestation)]
          (is (= 2 (get-in execution [:result :row_count])))
          (is (= :completed (get-in execution [:result :status])))
          (is (= (get-in attestation [:manifest :exact_pmbql_fingerprint])
                 (:executed_pmbql_fingerprint execution)))
          (is (= (get-in attestation [:manifest :attestation_id])
                 (:attestation_id execution))))))))

(deftest wrong-expected-fingerprint-blocks-before-native-db-execution-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "wrong-fingerprint"]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id
                        :query-id query-id
                        :query (count-star-query)
                        :user-id owner-id})
        (let [attestation (attest! convo-id query-id)]
          (with-redefs [qp/process-query
                        (fn [& _]
                          (throw (ex-info "DB execution must not happen"
                                          {:dima/error-code "DB_EXECUTION_REACHED"})))]
            (binding [dima.attestation/*runtime-identity-override* test-runtime]
              (is (= "NATIVE_QUERY_EXECUTION_FINGERPRINT_MISMATCH"
                     (exception-code
                      #(dima.execution/execute-native-query!
                        {:conversation_id (java.util.UUID/fromString convo-id)
                         :native_query_id query-id
                         :expected_pmbql_fingerprint (apply str (repeat 64 "0"))
                         :expected_attestation_id
                         (get-in attestation [:manifest :attestation_id])})))))))))))

(deftest wrong-current-user-blocks-before-execution-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          other-id (mt/user->id :lucky)
          convo-id (str (random-uuid))
          query-id "wrong-user"]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id
                        :query-id query-id
                        :query (count-star-query)
                        :user-id owner-id})
        (let [attestation (attest! convo-id query-id)]
          (binding [api/*current-user-id* other-id
                    dima.attestation/*runtime-identity-override* test-runtime]
            (is (= "NATIVE_ATTESTATION_SUBJECT_MISMATCH"
                   (exception-code
                    #(dima.execution/execute-native-query!
                      {:conversation_id (java.util.UUID/fromString convo-id)
                       :native_query_id query-id
                       :expected_pmbql_fingerprint
                       (get-in attestation [:manifest :exact_pmbql_fingerprint])
                       :expected_attestation_id
                       (get-in attestation [:manifest :attestation_id])}))))))))))

(deftest producer-state-mismatch-blocks-exact-occurrence-execution-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "producer-state-mismatch"]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id
                        :query-id query-id
                        :query (count-star-query)
                        :state-query (temporal-ranking-query)
                        :user-id owner-id})
        (binding [dima.attestation/*runtime-identity-override* test-runtime]
          (is (= "NATIVE_QUERY_STATE_MISMATCH"
                 (exception-code
                  #(dima.execution/execute-native-query!
                    {:conversation_id (java.util.UUID/fromString convo-id)
                     :native_query_id query-id
                     :expected_pmbql_fingerprint (apply str (repeat 64 "1"))
                     :expected_attestation_id "dima_att_expected"})))))))))

(deftest mutation-after-attestation-is-detected-before-execution-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "mutated-after-attestation"]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id
                        :query-id query-id
                        :query (count-star-query)
                        :user-id owner-id})
        (let [attestation (attest! convo-id query-id)]
          (t2/update! :model/MetabotConversation convo-id
                      {:state {:queries {query-id (temporal-ranking-query)}}})
          (is (= "NATIVE_QUERY_STATE_MISMATCH"
                 (exception-code #(execute! convo-id query-id attestation)))))))))

(deftest unsupported-temporal-representation-fails-closed-test
  (mt/test-driver :h2
    (is (= "NATIVE_QUERY_RUNTIME_ROUNDTRIP_MISMATCH"
           (exception-code
            #(dima.compat/restore-exact-runtime-query!
              (dima.attestation/exact-serialized-query
               (offset-temporal-query))))))))

(deftest non-temporal-p13b-shape-is-unchanged-by-compatibility-codec-test
  (mt/test-driver :h2
    (let [query (count-star-query)]
      (is (= (dima.attestation/exact-serialized-query query)
             (dima.attestation/exact-serialized-query
              (dima.compat/hydrate-runtime-query! query)))))))
