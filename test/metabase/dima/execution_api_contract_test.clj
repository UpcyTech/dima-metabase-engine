(ns metabase.dima.execution-api-contract-test
  (:require
   [clojure.test :refer :all]
   [metabase.dima.native-attestation :as dima.attestation]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.metabot.persistence :as metabot.persistence]
   [metabase.test :as mt]
   [metabase.test.fixtures :as fixtures]))

(use-fixtures :once (fixtures/initialize :db))

(def ^:private test-runtime
  {:repository          "UpcyTech/dima-metabase-engine"
   :revision_sha        "6666666666666666666666666666666666666666"
   :upstream_base_sha   "2ba2485c78d7e00a9a25f82c00fc201da71590c4"
   :runtime_tag         "v0.63.18-dima.6-test"
   :build_identity      "test-build:execution-api"
   :image_identity      "local-image:execution-api"
   :runtime_instance_id "00000000-0000-4000-8000-000000000067"})

(defn- query []
  (let [mp    (mt/metadata-provider)
        table (lib.metadata/table mp (mt/id :orders))]
    (-> (lib/query mp table)
        (lib/aggregate (lib/count)))))

(defn- persist! [conversation-id query-id owner-id]
  (let [q (query)
        {:keys [assistant-msg-id]}
        (metabot.persistence/start-turn!
         conversation-id "internal"
         {:role "user" :content "count"}
         :user-id owner-id)]
    (metabot.persistence/finalize-assistant-turn!
     conversation-id assistant-msg-id
     [{:type :tool-input :id "call-1" :function "construct_notebook_query" :arguments {}}
      {:type :tool-output :id "call-1"
       :result {:output "ok"
                :structured-output {:query-id query-id :query q}}}
      {:type :data :data-type "state"
       :data {:queries {query-id q}}}])))

(deftest exact-occurrence-execution-api-does-not-accept-caller-query-body-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "api-exact-occurrence"]
      (mt/with-current-user owner-id
        (persist! convo-id query-id owner-id)
        (binding [dima.attestation/*runtime-identity-override* test-runtime]
          (let [attestation (dima.attestation/attest-native-query!
                             {:conversation_id (java.util.UUID/fromString convo-id)
                              :native_query_id query-id})
                payload {:conversation_id convo-id
                         :native_query_id query-id
                         :expected_pmbql_fingerprint
                         (get-in attestation [:manifest :exact_pmbql_fingerprint])
                         :expected_attestation_id
                         (get-in attestation [:manifest :attestation_id])
                         :query (:exact_serialized_pmbql attestation)}]
            (is (= 400
                   (:status
                    (mt/user-http-request :rasta
                                          :post
                                          nil
                                          "dima/engine/v1/native-query-execution"
                                          payload))))))))))
