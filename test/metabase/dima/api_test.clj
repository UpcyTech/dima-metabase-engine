(ns metabase.dima.api-test
  (:require
   [clojure.test :refer :all]
   [metabase.dima.native-attestation :as dima.attestation]
   [metabase.dima.native-material-observation :as dima.material]
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
   :runtime_tag         "v0.63.18-dima.1-test"
   :build_identity      "test-build:api"
   :image_identity      "local-image:test"
   :runtime_instance_id "00000000-0000-4000-8000-000000000014"})

(defn- june-query []
  (let [mp         (mt/metadata-provider)
        table      (lib.metadata/table mp (mt/id :orders))
        created-at (lib.metadata/field mp (mt/id :orders :created_at))]
    (-> (lib/query mp table)
        (lib/aggregate (lib/count))
        (lib/filter (lib/>= created-at "2026-06-01"))
        (lib/filter (lib/< created-at "2026-07-01")))))

(defn- persist-query! [conversation-id query-id owner-id]
  (let [query (june-query)
        {:keys [assistant-msg-id]}
        (metabot.persistence/start-turn!
         conversation-id
         "internal"
         {:role "user" :content "June count"}
         :user-id owner-id)]
    (metabot.persistence/finalize-assistant-turn!
     conversation-id
     assistant-msg-id
     [{:type :tool-input
       :id "call-1"
       :function "construct_notebook_query"
       :arguments {}}
      {:type :tool-output
       :id "call-1"
       :result {:output "ok"
                :structured-output {:query-id query-id
                                    :query query}}}
      {:type :data
       :data-type "state"
       :data {:queries {query-id query}}}])
    query))

(deftest dima-engine-api-requires-authentication-test
  (is (= "Unauthenticated"
         (mt/client :get 401 "dima/engine/v1/identity")))
  (is (= "Unauthenticated"
         (mt/client :post 401
                    "dima/engine/v1/native-query-attestation"
                    {:conversation_id (str (random-uuid))
                     :native_query_id "q"})))
  (is (= "Unauthenticated"
         (mt/client :post 401
                    "dima/engine/v1/native-query-material-observation"
                    {:conversation_id (str (random-uuid))
                     :native_query_id "q"}))))

(deftest engine-identity-endpoint-returns-exact-dima-identity-test
  (with-redefs [dima.attestation/runtime-identity (constantly test-runtime)]
    (is (= test-runtime
           (mt/user-http-request :rasta :get 200 "dima/engine/v1/identity")))))

(deftest native-query-attestation-api-accepts-only-server-side-locators-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "api-native-q"]
      (mt/with-current-user owner-id
        (persist-query! convo-id query-id owner-id))
      (with-redefs [dima.attestation/runtime-identity (constantly test-runtime)]
        (let [response (mt/user-http-request
                        :rasta
                        :post
                        200
                        "dima/engine/v1/native-query-attestation"
                        {:conversation_id convo-id
                         :native_query_id query-id})]
          (is (= query-id (get-in response [:manifest :native_query_id])))
          (is (= owner-id (get-in response [:manifest :authenticated_metabase_subject])))
          (is (= "construct_notebook_query" (get-in response [:manifest :producer_tool])))
          (is (= (:exact_pmbql_fingerprint (:manifest response))
                 (dima.attestation/exact-query-fingerprint
                  (:exact_serialized_pmbql response))))))
      (testing "business expectations are not accepted as attestation input"
        (with-redefs [dima.attestation/runtime-identity (constantly test-runtime)]
          (mt/user-http-request
           :rasta
           :post
           400
           "dima/engine/v1/native-query-attestation"
           {:conversation_id convo-id
            :native_query_id query-id
            :expected_metric "sales_count"})))
      (testing "generic unknown keys are rejected at the raw request boundary"
        (with-redefs [dima.attestation/runtime-identity (constantly test-runtime)]
          (mt/user-http-request
           :rasta
           :post
           400
           "dima/engine/v1/native-query-attestation"
           {:conversation_id convo-id
            :native_query_id query-id
            :unexpected "not-accepted"}))))))


(deftest native-query-material-observation-api-accepts-only-occurrence-locators-test
  (let [convo-id (str (random-uuid))
        query-id "material-q"
        response {:schema_version "dima_native_material_observation_v1"
                  :conversation_id convo-id
                  :native_query_id query-id
                  :assistant_message_id 17
                  :tool_call_id "call-material"
                  :query_fingerprint (apply str (repeat 64 "a"))
                  :authenticated_metabase_subject 7
                  :database_id 1
                  :runtime_identity test-runtime
                  :native_metrics []
                  :dimensions []
                  :filters []
                  :temporal_scopes []
                  :ranking []}
        calls (atom [])]
    (with-redefs [dima.material/observe-native-query-material!
                  (fn [locator]
                    (swap! calls conj locator)
                    response)]
      (is (= response
             (mt/user-http-request
              :rasta
              :post
              200
              "dima/engine/v1/native-query-material-observation"
              {:conversation_id convo-id
               :native_query_id query-id})))
      (is (= [{:conversation_id (java.util.UUID/fromString convo-id)
               :native_query_id query-id}]
             @calls))
      (testing "caller query/business expectations are rejected at the raw boundary"
        (doseq [extra [{:query {:database 1}}
                       {:expected_metric "sales_count"}
                       {:expected_dates ["2026-06-01" "2026-07-01"]}
                       {:scope_version "scope_v1"}
                       {:benchmark_id "case-1"}]]
          (mt/user-http-request
           :rasta
           :post
           400
           "dima/engine/v1/native-query-material-observation"
           (merge {:conversation_id convo-id
                   :native_query_id query-id}
                  extra)))))))

(deftest participant-and-superuser-cannot-use-official-v1-attestation-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "subject-q"]
      (mt/with-current-user owner-id
        (persist-query! convo-id query-id owner-id))
      (with-redefs [dima.attestation/runtime-identity (constantly test-runtime)]
        (mt/user-http-request
         :lucky
         :post
         403
         "dima/engine/v1/native-query-attestation"
         {:conversation_id convo-id
          :native_query_id query-id})
        (mt/user-http-request
         :crowberto
         :post
         403
         "dima/engine/v1/native-query-attestation"
         {:conversation_id convo-id
          :native_query_id query-id})))))
