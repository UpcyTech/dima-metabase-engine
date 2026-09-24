(ns metabase.dima.native-temporal-repro-test
  (:require
   [clojure.test :refer :all]
   [metabase.lib-be.models.transforms :as lib-be.transforms]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.lib.serialize :as lib.serialize]
   [metabase.query-processor.preprocess :as qp.preprocess]
   [metabase.test :as mt]
   [metabase.test.fixtures :as fixtures]))

(use-fixtures :once (fixtures/initialize :db))

(defn- native-ranking-query []
  (let [mp         (mt/metadata-provider)
        orders     (lib.metadata/table mp (mt/id :orders))
        created-at (lib.metadata/field mp (mt/id :orders :created_at))
        product-id (lib.metadata/field mp (mt/id :orders :product_id))
        lower      (lib/absolute-datetime
                    (java.time.LocalDateTime/parse "2026-06-01T00:00:00")
                    :day)
        upper      (lib/absolute-datetime
                    (java.time.LocalDateTime/parse "2026-06-30T23:59:59")
                    :day)
        query      (-> (lib/query mp orders)
                       (lib/aggregate (lib/count))
                       (lib/filter (lib/between created-at lower upper))
                       (lib/breakout product-id))]
    (-> query
        (lib/order-by (lib/aggregation-ref query 0) :desc)
        (lib/limit 2))))

(deftest clean-native-api-boundary-reproduces-persisted-absolute-datetime-failure-test
  (testing "clean pinned native API/app-DB normalization still feeds serialized absolute-datetime strings to QP"
    (mt/test-driver :h2
      (let [query       (native-ranking-query)
            persisted   (lib.serialize/prepare-for-serialization query)
            native-in   (-> (lib-be.transforms/normalize-query nil persisted {:strict? true})
                            lib.serialize/prepare-after-deserialization)
            abs-clauses (->> (tree-seq coll? seq native-in)
                             (filter #(and (vector? %)
                                           (= :absolute-datetime (first %))))
                             vec)]
        (is (= 2 (count abs-clauses)))
        (is (every? string? (map #(nth % 2) abs-clauses)))
        (is (thrown-with-msg?
             IllegalArgumentException
             #"No implementation of method: :truncate-to.*java.lang.String"
             (qp.preprocess/preprocess native-in)))))))
