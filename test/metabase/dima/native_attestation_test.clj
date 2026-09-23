(ns metabase.dima.native-attestation-test
  (:require
   [clojure.test :refer :all]
   [metabase.api.common :as api]
   [metabase.dima.native-attestation :as dima.attestation]
   [metabase.lib.core :as lib]
   [metabase.lib.filter :as lib.filter]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.lib.test-util :as lib.tu]
   [metabase.lib.test-metadata :as meta]
   [metabase.lib.test-util.macros :as lib.tu.macros]
   [metabase.metabot.persistence :as metabot.persistence]
   [metabase.query-processor :as qp]
   [metabase.query-processor.middleware.add-implicit-joins :as qp.add-implicit-joins]
   [metabase.query-processor.middleware.desugar :as qp.desugar]
   [metabase.query-processor.middleware.fetch-source-query :as qp.fetch-source-query]
   [metabase.query-processor.middleware.metrics :as qp.metrics]
   [metabase.query-processor.middleware.permissions :as qp.perms]
   [metabase.test :as mt]
   [metabase.test.fixtures :as fixtures]
   [toucan2.core :as t2]))

; P13B native metric provider-free probe marker: no production behavior.
(use-fixtures :once (fixtures/initialize :db))

(def ^:private test-runtime
  {:repository          "UpcyTech/dima-metabase-engine"
   :revision_sha        "1111111111111111111111111111111111111111"
   :upstream_base_sha   "2ba2485c78d7e00a9a25f82c00fc201da71590c4"
   :runtime_tag         "v0.63.18-dima.1-test"
   :build_identity      "test-build:1"
   :image_identity      "local-image:test"
   :runtime_instance_id "00000000-0000-4000-8000-000000000013"})

(defn- exception-code [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      (:dima/error-code (ex-data e)))))

(defn- orders-query []
  (let [mp (mt/metadata-provider)]
    (lib/query mp (lib.metadata/table mp (mt/id :orders)))))

(defn- count-star-query []
  (lib/aggregate (orders-query) (lib/count)))

(defn- count-field-query []
  (let [mp (mt/metadata-provider)]
    (lib/aggregate
     (orders-query)
     (lib/count (lib.metadata/field mp (mt/id :orders :created_at))))))

(defn- products-count-query []
  (let [mp (mt/metadata-provider)]
    (lib/aggregate
     (lib/query mp (lib.metadata/table mp (mt/id :products)))
     (lib/count))))

(defn- product-category-equality-query [value]
  (let [mp       (mt/metadata-provider)
        category (lib.metadata/field mp (mt/id :products :category))]
    (lib/filter (products-count-query) (lib/= category value))))

(defn- june-count-query []
  (let [mp         (mt/metadata-provider)
        created-at (lib.metadata/field mp (mt/id :orders :created_at))]
    (-> (count-star-query)
        (lib/filter (lib/>= created-at "2026-06-01"))
        (lib/filter (lib/< created-at "2026-07-01")))))

(defn- june-during-query []
  (let [mp         (mt/metadata-provider)
        created-at (lib.metadata/field mp (mt/id :orders :created_at))]
    (lib/filter
     (count-star-query)
     (lib.filter/during created-at "2026-06-01" :month))))

(def ^:private probe-metric-id 900001)
(def ^:private probe-metric-entity-id "p13bmetricprobe000000001")

(defn- metric-probe-query
  [aggregation-fn]
  (let [base-mp          (mt/metadata-provider)
        orders           (lib.metadata/table base-mp (mt/id :orders))
        definition-query (lib/aggregate (lib/query base-mp orders)
                                        (aggregation-fn base-mp))
        metric           {:lib/type      :metadata/card
                          :id            probe-metric-id
                          :entity-id     probe-metric-entity-id
                          :database-id   (mt/id)
                          :table-id      (mt/id :orders)
                          :name          "P13B Probe Metric"
                          :type          :metric
                          :dataset-query definition-query}
        mp               (lib/composed-metadata-provider
                          base-mp
                          (lib.tu/mock-metadata-provider {:cards [metric]}))
        query            (-> (lib/query mp (lib.metadata/table mp (mt/id :orders)))
                             (lib/aggregate (lib.metadata/metric mp probe-metric-id)))]
    {:metric metric
     :query query}))

(defn- metric-observation-view
  [query]
  (-> query
      (assoc-in [:info :pivot/original-query] query)
      (#'qp.fetch-source-query/resolve-source-cards)
      (#'qp.metrics/adjust)
      (dissoc :info)))

(defn- tool-parts
  [{:keys [query-id query call-id producer]
    :or {call-id "tool-call-1" producer "construct_notebook_query"}}]
  [{:type      :tool-input
    :id        call-id
    :function  producer
    :arguments {}}
   {:type   :tool-output
    :id     call-id
    :result {:output            "ok"
             :structured-output {:query-id query-id
                                 :query    query}}}])

(defn- persist-turn!
  [{:keys [conversation-id query-id query user-id producer finished? error extra-parts state-query]
    :or {producer "construct_notebook_query"
         finished? true}}]
  (let [{:keys [assistant-msg-id]}
        (metabot.persistence/start-turn!
         conversation-id
         "internal"
         {:role "user" :content "test"}
         :user-id user-id)
        parts (vec
               (concat
                (tool-parts {:query-id query-id
                             :query query
                             :producer producer})
                extra-parts
                [{:type :data
                  :data-type "state"
                  :data {:queries {query-id (or state-query query)}}}]))]
    (metabot.persistence/finalize-assistant-turn!
     conversation-id
     assistant-msg-id
     parts
     :finished? finished?
     :error error)
    assistant-msg-id))

(deftest canonical-json-fingerprint-cross-language-fixture-test
  (testing "canonical JSON ordering matches the frozen Python-side algorithm"
    (let [fixture {"z" [3 {"b" true "a" "İ"}]
                   "a" {"y" nil "x" 1}}]
      (is (= "{\"a\":{\"x\":1,\"y\":null},\"z\":[3,{\"a\":\"İ\",\"b\":true}]}"
             (dima.attestation/canonical-json fixture)))
      (is (= "476964df49543fc2c839aa3bc698b01db42f168591d7563747453cf560d57e24"
             (#'dima.attestation/sha256-hex
              (dima.attestation/canonical-json fixture)))))))

(deftest aggregation-facts-distinguish-count-star-from-count-field-test
  (testing "COUNT(*) is not conflated with COUNT(field)"
    (mt/test-driver :h2
      (let [star  (first (#'dima.attestation/aggregation-facts (count-star-query)))
            field (first (#'dima.attestation/aggregation-facts (count-field-query)))]
        (is (= {:operator "count"
                :argument_kind "all_rows"
                :referenced_field_ids []
                :distinct false}
               star))
        (is (= "count" (:operator field)))
        (is (= "field" (:argument_kind field)))
        (is (= [(mt/id :orders :created_at)] (:referenced_field_ids field)))
        (is (false? (:distinct field)))))))

(deftest temporal-facts-observe-literal-june-half-open-bounds-test
  (testing "engine observes the actual lower and upper predicates rather than copying an expected period"
    (mt/test-driver :h2
      (let [facts (#'dima.attestation/filter-facts (june-count-query))
            predicates (:temporal_predicates facts)]
        (is (= 2 (:material_filter_count facts)))
        (is (= 0 (:non_temporal_filter_count facts)))
        (is (= 2 (count predicates)))
        (is (= #{(mt/id :orders :created_at)}
               (set (map :time_field_id predicates))))
        (is (some #(and (= "2026-06-01" (:lower_bound %))
                        (true? (:lower_inclusive %)))
                  predicates))
        (is (some #(and (= "2026-07-01" (:upper_bound %))
                        (false? (:upper_inclusive %)))
                  predicates))))))


(deftest during-original-view-is-not-certified-as-literal-bounds-test
  (testing "the live native :during shape reproduces the current P13B-v1 attestation gap"
    (mt/test-driver :h2
      (is (= "NATIVE_TEMPORAL_SHAPE_UNSUPPORTED"
             (exception-code
              (fn []
                (#'dima.attestation/filter-facts (june-during-query)))))))))

(deftest during-native-preprocess-resolves-exact-june-half-open-bounds-test
  (testing "native QP desugar resolves :during to exact physical half-open bounds without mutating execution identity"
    (mt/test-driver :h2
      (let [query        (june-during-query)
            exact-before (#'dima.attestation/exact-serialized-query query)
            fingerprint  (dima.attestation/exact-query-fingerprint query)
            normalized   (qp.desugar/desugar query)
            facts        (#'dima.attestation/filter-facts normalized)
            predicates   (:temporal_predicates facts)]
        (is (= 2 (:material_filter_count facts)))
        (is (= 0 (:non_temporal_filter_count facts)))
        (is (= 2 (count predicates)))
        (is (= #{(mt/id :orders :created_at)}
               (set (map :time_field_id predicates))))
        (is (some #(and (= "2026-06-01" (:lower_bound %))
                        (true? (:lower_inclusive %)))
                  predicates))
        (is (some #(and (= "2026-07-01" (:upper_bound %))
                        (false? (:upper_inclusive %)))
                  predicates))
        (is (= exact-before
               (#'dima.attestation/exact-serialized-query query)))
        (is (= fingerprint
               (dima.attestation/exact-query-fingerprint query)))))))

(deftest non-temporal-filter-is-observed-as-material-query-fact-test
  (mt/test-driver :h2
    (let [mp       (mt/metadata-provider)
          quantity (lib.metadata/field mp (mt/id :orders :quantity))
          query    (lib/filter (count-star-query) (lib/> quantity 1))
          facts    (#'dima.attestation/filter-facts query)]
      (is (= 1 (:material_filter_count facts)))
      (is (= 1 (:non_temporal_filter_count facts)))
      (is (empty? (:temporal_predicates facts)))
      (is (empty? (:textual_equality_predicates facts))))))

(deftest scalar-textual-equality-is-attested-with-exact-native-facts-test
  (mt/test-driver :h2
    (let [query (product-category-equality-query "Gizmo")
          facts (#'dima.attestation/filter-facts query)
          predicate (first (:textual_equality_predicates facts))]
      (is (= 1 (:material_filter_count facts)))
      (is (= 1 (:non_temporal_filter_count facts)))
      (is (empty? (:temporal_predicates facts)))
      (is (= 1 (count (:textual_equality_predicates facts))))
      (is (= 0 (:stage_number predicate)))
      (is (= (mt/id :products :category) (:field_id predicate)))
      (is (= "=" (:operator predicate)))
      (is (= "Gizmo" (:literal_value predicate)))
      (is (string? (:field_type predicate))))))

(deftest textual-equality-literal-preserves-exact-case-test
  (mt/test-driver :h2
    (let [upper (first (:textual_equality_predicates
                        (#'dima.attestation/filter-facts
                         (product-category-equality-query "Gizmo"))))
          lower (first (:textual_equality_predicates
                        (#'dima.attestation/filter-facts
                         (product-category-equality-query "gizmo"))))]
      (is (= "Gizmo" (:literal_value upper)))
      (is (= "gizmo" (:literal_value lower)))
      (is (not= (:literal_value upper) (:literal_value lower))))))

(deftest explicit-join-count-is-observed-from-lib-test
  (mt/test-driver :h2
    (let [mp         (mt/metadata-provider)
          products   (lib.metadata/table mp (mt/id :products))
          product-id (lib.metadata/field mp (mt/id :orders :product_id))
          products-id (lib.metadata/field mp (mt/id :products :id))
          query      (lib/join
                      (orders-query)
                      (lib/join-clause products [(lib/= product-id products-id)]))]
      (is (= 1 (#'dima.attestation/explicit-join-count query))))))

(deftest implicit-join-is-observed-after-native-qp-implicit-join-middleware-test
  (testing "QP-created implicit joins are observable and carry the native marker"
    (let [query (lib/query
                 meta/metadata-provider
                 (lib.tu.macros/mbql-query orders
                   {:source-table $$orders
                    :fields [$id $product-id->products.title]}))
          preprocessed (qp.add-implicit-joins/add-implicit-joins query)
          implicit (#'dima.attestation/implicit-joins preprocessed)]
      (is (= 1 (count implicit)))
      (is (true? (:qp/is-implicit-join (first implicit))))
      (is (= [(meta/id :products)]
             (mapv #'dima.attestation/implicit-joined-table-id implicit))))))

(deftest runtime-identity-requires-full-source-and-deployment-facts-test
  (testing "missing identity fails closed"
    (binding [dima.attestation/*env-reader* (constantly nil)]
      (is (= "ENGINE_IDENTITY_INCOMPLETE"
             (exception-code dima.attestation/runtime-identity)))))
  (testing "short revision SHA is rejected"
    (let [env {"DIMA_ENGINE_REPOSITORY" "UpcyTech/dima-metabase-engine"
               "DIMA_ENGINE_REVISION_SHA" "abcdef0"
               "DIMA_ENGINE_UPSTREAM_BASE_SHA" "2ba2485c78d7e00a9a25f82c00fc201da71590c4"
               "DIMA_ENGINE_RUNTIME_TAG" "v0.63.18-dima.1"
               "DIMA_ENGINE_BUILD_IDENTITY" "build"
               "DIMA_ENGINE_IMAGE_IDENTITY" "local-image:test"}]
      (binding [dima.attestation/*env-reader* #(get env %)]
        (is (= "ENGINE_IDENTITY_INVALID"
               (exception-code dima.attestation/runtime-identity))))))
  (testing "runtime instance identity is mandatory"
    (binding [dima.attestation/*runtime-identity-override*
              (dissoc test-runtime :runtime_instance_id)]
      (is (= "ENGINE_IDENTITY_INCOMPLETE"
             (exception-code dima.attestation/runtime-identity)))))
  (testing "runtime instance identity is stable for one process"
    (binding [dima.attestation/*runtime-identity-override* test-runtime]
      (is (= (:runtime_instance_id (dima.attestation/runtime-identity))
             (:runtime_instance_id (dima.attestation/runtime-identity)))))))

(deftest exact-native-occurrence-attests-under-originator-and-current-permissions-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "native-q-1"]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id
                        :query-id query-id
                        :query (june-count-query)
                        :user-id owner-id})
        (binding [dima.attestation/*runtime-identity-override* test-runtime]
          (let [{:keys [exact_serialized_pmbql manifest]}
                (dima.attestation/attest-native-query!
                 {:conversation_id (java.util.UUID/fromString convo-id)
                  :native_query_id query-id})]
            (is (= query-id (:native_query_id manifest)))
            (is (= "construct_notebook_query" (:producer_tool manifest)))
            (is (= owner-id (:authenticated_metabase_subject manifest)))
            (is (= 1 (:aggregation_count manifest)))
            (is (= "all_rows" (get-in manifest [:aggregations 0 :argument_kind])))
            (is (= 2 (:material_filter_count manifest)))
            (is (= 0 (:non_temporal_filter_count manifest)))
            (is (empty? (:textual_equality_predicates manifest)))
            (is (= 0 (:explicit_join_count manifest)))
            (is (= 0 (:implicit_join_count manifest)))
            (is (= 1 (:material_query_count manifest)))
            (is (= "PASSED" (get-in manifest [:permission_provenance :permission_check])))
            (is (= (:exact_pmbql_fingerprint manifest)
                   (dima.attestation/exact-query-fingerprint exact_serialized_pmbql)))))))))

(deftest during-native-occurrence-attests-from-desugared-observation-with-original-identity-test
  (testing "P13B observes native :during through Metabase desugar while preserving the exact original execution artifact"
    (mt/test-driver :h2
      (let [owner-id (mt/user->id :rasta)
            convo-id (str (random-uuid))
            query-id "native-during-q"
            query    (june-during-query)
            exact    (#'dima.attestation/exact-serialized-query query)
            expected-fingerprint (dima.attestation/exact-query-fingerprint query)]
        (mt/with-current-user owner-id
          (persist-turn! {:conversation-id convo-id
                          :query-id query-id
                          :query query
                          :user-id owner-id})
          (binding [dima.attestation/*runtime-identity-override* test-runtime]
            (let [{:keys [exact_serialized_pmbql manifest]}
                  (dima.attestation/attest-native-query!
                   {:conversation_id (java.util.UUID/fromString convo-id)
                    :native_query_id query-id})
                  predicates (:temporal_predicates manifest)]
              (is (= exact exact_serialized_pmbql))
              (is (= expected-fingerprint (:exact_pmbql_fingerprint manifest)))
              (is (= expected-fingerprint
                     (dima.attestation/exact-query-fingerprint exact_serialized_pmbql)))
              (is (= 2 (:material_filter_count manifest)))
              (is (= 0 (:non_temporal_filter_count manifest)))
              (is (= 2 (count predicates)))
              (is (= #{(mt/id :orders :created_at)}
                     (set (map :time_field_id predicates))))
              (is (some #(and (= ">=" (:operator %))
                              (= "2026-06-01" (:lower_bound %))
                              (true? (:lower_inclusive %)))
                        predicates))
              (is (some #(and (= "<" (:operator %))
                              (= "2026-07-01" (:upper_bound %))
                              (false? (:upper_inclusive %)))
                        predicates)))))))))

(deftest repeated-attestation-of-one-persisted-occurrence-is-idempotent-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "idempotent-native-q"]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id
                        :query-id query-id
                        :query (june-count-query)
                        :user-id owner-id})
        (binding [dima.attestation/*runtime-identity-override* test-runtime]
          (let [first-attestation
                (dima.attestation/attest-native-query!
                 {:conversation_id (java.util.UUID/fromString convo-id)
                  :native_query_id query-id})
                second-attestation
                (dima.attestation/attest-native-query!
                 {:conversation_id (java.util.UUID/fromString convo-id)
                  :native_query_id query-id})]
            (is (= (:exact_serialized_pmbql first-attestation)
                   (:exact_serialized_pmbql second-attestation)))
            (is (= (get-in first-attestation [:manifest :exact_pmbql_fingerprint])
                   (get-in second-attestation [:manifest :exact_pmbql_fingerprint])))
            (is (= (get-in first-attestation [:manifest :attestation_id])
                   (get-in second-attestation [:manifest :attestation_id])))))))))

(deftest native-occurrence-must-be-unique-finalized-and-state-consistent-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)]
      (testing "missing producer occurrence"
        (let [convo-id (str (random-uuid))]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id "other"
                            :query (count-star-query)
                            :user-id owner-id})
            (is (= "NATIVE_QUERY_OCCURRENCE_NOT_FOUND"
                   (exception-code
                    #(binding [dima.attestation/*runtime-identity-override* test-runtime]
                       (dima.attestation/attest-native-query!
                        {:conversation_id (java.util.UUID/fromString convo-id)
                         :native_query_id "missing"}))))))))
      (testing "ambiguous producer occurrence"
        (let [convo-id (str (random-uuid))
              query-id "same-id"]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id :query-id query-id
                            :query (count-star-query) :user-id owner-id})
            (persist-turn! {:conversation-id convo-id :query-id query-id
                            :query (count-star-query) :user-id owner-id})
            (is (= "NATIVE_QUERY_OCCURRENCE_AMBIGUOUS"
                   (exception-code
                    #(binding [dima.attestation/*runtime-identity-override* test-runtime]
                       (dima.attestation/attest-native-query!
                        {:conversation_id (java.util.UUID/fromString convo-id)
                         :native_query_id query-id}))))))))
      (testing "producer/state disagreement"
        (let [convo-id (str (random-uuid))
              query-id "state-mismatch"]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id :query-id query-id
                            :query (count-star-query)
                            :state-query (count-field-query)
                            :user-id owner-id})
            (is (= "NATIVE_QUERY_STATE_MISMATCH"
                   (exception-code
                    #(binding [dima.attestation/*runtime-identity-override* test-runtime]
                       (dima.attestation/attest-native-query!
                        {:conversation_id (java.util.UUID/fromString convo-id)
                         :native_query_id query-id}))))))))
      (testing "unfinished producer turn"
        (let [convo-id (str (random-uuid))
              query-id "unfinished"]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id :query-id query-id
                            :query (count-star-query) :user-id owner-id
                            :finished? false})
            (is (= "NATIVE_QUERY_PRODUCER_INVALID"
                   (exception-code
                    #(binding [dima.attestation/*runtime-identity-override* test-runtime]
                       (dima.attestation/attest-native-query!
                        {:conversation_id (java.util.UUID/fromString convo-id)
                         :native_query_id query-id}))))))))
      (testing "errored producer turn"
        (let [convo-id (str (random-uuid))
              query-id "errored"]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id :query-id query-id
                            :query (count-star-query) :user-id owner-id
                            :error {:message "producer failed"}})
            (is (= "NATIVE_QUERY_PRODUCER_INVALID"
                   (exception-code
                    #(binding [dima.attestation/*runtime-identity-override* test-runtime]
                       (dima.attestation/attest-native-query!
                        {:conversation_id (java.util.UUID/fromString convo-id)
                         :native_query_id query-id})))))))))))

(deftest p13b-v1-subject-policy-is-narrower-than-conversation-read-policy-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          other-id (mt/user->id :lucky)
          super-id (mt/user->id :crowberto)]
      (testing "participant-but-not-originator is blocked"
        (let [convo-id (str (random-uuid))
              query-id "owned-q"]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id :query-id query-id
                            :query (count-star-query) :user-id owner-id}))
          (binding [api/*current-user-id* other-id
                    dima.attestation/*runtime-identity-override* test-runtime]
            (is (= "NATIVE_ATTESTATION_SUBJECT_MISMATCH"
                   (exception-code
                    #(dima.attestation/attest-native-query!
                      {:conversation_id (java.util.UUID/fromString convo-id)
                       :native_query_id query-id})))))))
      (testing "superuser fallback is not accepted for official attestation"
        (let [convo-id (str (random-uuid))
              query-id "owned-super-q"]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id :query-id query-id
                            :query (count-star-query) :user-id owner-id}))
          (binding [api/*current-user-id* super-id
                    api/*is-superuser?* true
                    dima.attestation/*runtime-identity-override* test-runtime]
            (is (= "NATIVE_ATTESTATION_SUBJECT_MISMATCH"
                   (exception-code
                    #(dima.attestation/attest-native-query!
                      {:conversation_id (java.util.UUID/fromString convo-id)
                       :native_query_id query-id})))))))
      (testing "shared multi-user conversation is unsupported even for originator"
        (let [convo-id (str (random-uuid))
              query-id "shared-q"]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id :query-id query-id
                            :query (count-star-query) :user-id owner-id}))
          (mt/with-current-user other-id
            (metabot.persistence/start-turn!
             convo-id "internal" {:role "user" :content "participant"} :user-id other-id))
          (binding [api/*current-user-id* owner-id
                    dima.attestation/*runtime-identity-override* test-runtime]
            (is (= "SHARED_CONVERSATION_ATTESTATION_UNSUPPORTED"
                   (exception-code
                    #(dima.attestation/attest-native-query!
                      {:conversation_id (java.util.UUID/fromString convo-id)
                       :native_query_id query-id}))))))))))

(deftest subject-mismatch-blocks-before-message-content-load-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          other-id (mt/user->id :lucky)
          convo-id (str (random-uuid))
          query-id "pre-auth-q"]
      (mt/with-current-user owner-id
        (persist-turn! {:conversation-id convo-id
                        :query-id query-id
                        :query (count-star-query)
                        :user-id owner-id}))
      (binding [api/*current-user-id* other-id
                dima.attestation/*runtime-identity-override* test-runtime]
        (with-redefs [t2/select
                      (fn [& _]
                        (throw (ex-info "message content loaded before subject authorization"
                                        {:dima/error-code "PRE_AUTH_MESSAGE_LOAD"})))]
          (is (= "NATIVE_ATTESTATION_SUBJECT_MISMATCH"
                 (exception-code
                  #(dima.attestation/attest-native-query!
                    {:conversation_id (java.util.UUID/fromString convo-id)
                     :native_query_id query-id})))))))))

(deftest material-query-count-observes-second-query-and-sql-producer-is-not-certifiable-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)]
      (testing "a second successful analytical query occurrence is counted"
        (let [convo-id (str (random-uuid))
              query-id "notebook-q"
              sql-id "sql-q"
              sql-query {:database (mt/id)
                         :type :native
                         :native {:query "select 1"}}]
          (mt/with-current-user owner-id
            (persist-turn!
             {:conversation-id convo-id
              :query-id query-id
              :query (count-star-query)
              :user-id owner-id
              :extra-parts (tool-parts {:query-id sql-id
                                        :query sql-query
                                        :call-id "sql-call"
                                        :producer "create_sql_query"})})
            (binding [dima.attestation/*runtime-identity-override* test-runtime]
              (let [out (dima.attestation/attest-native-query!
                         {:conversation_id (java.util.UUID/fromString convo-id)
                          :native_query_id query-id})]
                (is (= 2 (get-in out [:manifest :material_query_count]))))))))
      (testing "SQL producer cannot be the requested P13B-v1 producer"
        (let [convo-id (str (random-uuid))
              query-id "sql-only"
              sql-query {:database (mt/id)
                         :type :native
                         :native {:query "select 1"}}]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query sql-query
                            :producer "create_sql_query"
                            :user-id owner-id})
            (is (= "NATIVE_QUERY_PRODUCER_INVALID"
                   (exception-code
                    #(binding [dima.attestation/*runtime-identity-override* test-runtime]
                       (dima.attestation/attest-native-query!
                        {:conversation_id (java.util.UUID/fromString convo-id)
                         :native_query_id query-id})))))))))))

(deftest permission-failure-is-not-converted-to-attestation-success-test
  (mt/test-driver :h2
    (with-redefs [qp.perms/check-query-permissions*
                  (fn [_]
                    (throw (ex-info "denied" {:status-code 403})))]
      (let [error (try
                    (#'dima.attestation/preprocess-and-authorize! (count-star-query))
                    nil
                    (catch clojure.lang.ExceptionInfo e e))]
        (is (= 403 (:status-code (ex-data error))))))))

(deftest native-metric-count-star-expansion-is-observation-only-test
  (testing "native metric identity survives in original pMBQL while Metabase expands its COUNT(*) definition for observation"
    (let [{:keys [query]} (metric-probe-query (fn [_] (lib/count)))
          exact-before    (#'dima.attestation/exact-serialized-query query)
          fingerprint     (dima.attestation/exact-query-fingerprint query)
          original-aggs   (lib/aggregations query)
          metric-meta     (lib.metadata/metric query probe-metric-id)
          observed        (metric-observation-view query)
          facts           (#'dima.attestation/aggregation-facts observed)]
      (is (= 1 (count original-aggs)))
      (is (= :metric (first (first original-aggs))))
      (is (= probe-metric-id (nth (first original-aggs) 2)))
      (is (= probe-metric-entity-id (:entity-id metric-meta)))
      (is (= [{:operator "count"
               :argument_kind "all_rows"
               :referenced_field_ids []
               :distinct false}]
             facts))
      (is (= exact-before (#'dima.attestation/exact-serialized-query query)))
      (is (= fingerprint (dima.attestation/exact-query-fingerprint query))))))

(deftest native-metric-count-field-does-not-expand-to-count-star-test
  (testing "COUNT(field) remains distinguishable from canonical COUNT(*) after native metric expansion"
    (let [{:keys [query]}
          (metric-probe-query
           (fn [mp]
             (lib/count (lib.metadata/field mp (mt/id :orders :created_at)))))
          facts (#'dima.attestation/aggregation-facts (metric-observation-view query))]
      (is (= 1 (count facts)))
      (is (= "count" (:operator (first facts))))
      (is (= "field" (:argument_kind (first facts))))
      (is (= [(mt/id :orders :created_at)]
             (:referenced_field_ids (first facts))))
      (is (false? (:distinct (first facts)))))))

(deftest native-metric-distinct-field-does-not-expand-to-count-star-test
  (testing "DISTINCT(field) remains distinguishable from canonical COUNT(*) after native metric expansion"
    (let [{:keys [query]}
          (metric-probe-query
           (fn [mp]
             (lib/distinct (lib.metadata/field mp (mt/id :orders :created_at)))))
          facts (#'dima.attestation/aggregation-facts (metric-observation-view query))]
      (is (= 1 (count facts)))
      (is (= "distinct" (:operator (first facts))))
      (is (= "field" (:argument_kind (first facts))))
      (is (= [(mt/id :orders :created_at)]
             (:referenced_field_ids (first facts))))
      (is (true? (:distinct (first facts)))))))

(deftest native-metric-observation-contract-retains-reference-and-expanded-facts-test
  (let [{:keys [query]} (metric-probe-query (fn [_] (lib/count)))
        exact-before     (#'dima.attestation/exact-serialized-query query)
        fingerprint      (dima.attestation/exact-query-fingerprint query)
        refs             (#'dima.attestation/native-metric-references query)
        observed         (metric-observation-view query)
        facts            (#'dima.attestation/attested-aggregation-facts query observed refs)]
    (is (= [{:stage_number 0
             :aggregation_index 0
             :metabase_metric_id probe-metric-id
             :metabase_metric_entity_id probe-metric-entity-id}]
           refs))
    (is (= [{:operator "count"
             :argument_kind "all_rows"
             :referenced_field_ids []
             :distinct false}]
           facts))
    (is (= exact-before (#'dima.attestation/exact-serialized-query query)))
    (is (= fingerprint (dima.attestation/exact-query-fingerprint query)))))

(deftest persisted-native-metric-occurrence-attests-with-original-query-identity-test
  (mt/test-driver :h2
    (let [owner-id (mt/user->id :rasta)
          convo-id (str (random-uuid))
          query-id "native-metric-q"
          definition (count-star-query)]
      (mt/with-temp
        [:model/Card
         {metric-id :id metric-entity-id :entity_id}
         {:name "P13B Native Metric"
          :type :metric
          :database_id (mt/id)
          :table_id (mt/id :orders)
          :dataset_query definition}]
        (let [mp (mt/metadata-provider)
              created-at (lib.metadata/field mp (mt/id :orders :created_at))
              query (-> (lib/query mp (lib.metadata/table mp (mt/id :orders)))
                        (lib/aggregate (lib.metadata/metric mp metric-id))
                        (lib/filter (lib.filter/during created-at "2026-06-01" :month)))
              exact (#'dima.attestation/exact-serialized-query query)
              fingerprint (dima.attestation/exact-query-fingerprint query)]
          (mt/with-current-user owner-id
            (persist-turn! {:conversation-id convo-id
                            :query-id query-id
                            :query query
                            :user-id owner-id})
            (binding [dima.attestation/*runtime-identity-override* test-runtime]
              (let [{:keys [exact_serialized_pmbql manifest]}
                    (dima.attestation/attest-native-query!
                     {:conversation_id (java.util.UUID/fromString convo-id)
                      :native_query_id query-id})]
                (is (= exact exact_serialized_pmbql))
                (is (= fingerprint (:exact_pmbql_fingerprint manifest)))
                (is (= [{:operator "count"
                         :argument_kind "all_rows"
                         :referenced_field_ids []
                         :distinct false}]
                       (:aggregations manifest)))
                (is (= [{:stage_number 0
                         :aggregation_index 0
                         :metabase_metric_id metric-id
                         :metabase_metric_entity_id metric-entity-id}]
                       (:native_metric_references manifest)))
                (is (= 2 (:material_filter_count manifest)))
                (is (= 2 (count (:temporal_predicates manifest))))
                (is (= fingerprint
                       (dima.attestation/exact-query-fingerprint exact_serialized_pmbql)))))))))))

(deftest native-metric-expanded-count-field-is-not-count-star-test
  (let [{:keys [query]}
        (metric-probe-query
         (fn [mp]
           (lib/count (lib.metadata/field mp (mt/id :orders :created_at)))))
        refs (#'dima.attestation/native-metric-references query)
        facts (#'dima.attestation/attested-aggregation-facts
               query
               (metric-observation-view query)
               refs)]
    (is (= "count" (:operator (first facts))))
    (is (= "field" (:argument_kind (first facts))))
    (is (= [(mt/id :orders :created_at)]
           (:referenced_field_ids (first facts))))))

(deftest native-metric-expanded-distinct-field-is-not-count-star-test
  (let [{:keys [query]}
        (metric-probe-query
         (fn [mp]
           (lib/distinct (lib.metadata/field mp (mt/id :orders :created_at)))))
        refs (#'dima.attestation/native-metric-references query)
        facts (#'dima.attestation/attested-aggregation-facts
               query
               (metric-observation-view query)
               refs)]
    (is (= "distinct" (:operator (first facts))))
    (is (= "field" (:argument_kind (first facts))))
    (is (true? (:distinct (first facts))))))
