(ns metabase.dima.native-query-compat
  "Dima-owned compatibility codec for exact serialized pMBQL runtime representation.

  Canonical authority is the exact serialized artifact A. This namespace may restore only
  a narrowly certified runtime representation H(A), and it must prove serialize(H(A)) == A
  before returning a query to native Metabase QP. It contains no analytical semantics."
  (:require
   [clojure.walk :as walk]
   [metabase.lib-be.core :as lib-be]
   [metabase.lib.normalize :as lib.normalize]
   [metabase.lib.schema :as lib.schema]
   [metabase.lib.serialize :as lib.serialize]
   [metabase.util.json :as json]
   [metabase.util.time :as u.time])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(defn- fail!
  [code status message data]
  (throw (ex-info message
                  (merge {:status-code status
                          :dima/error-code code}
                         data))))

(defn- json-wire-value [value]
  (json/decode (json/encode value)))

(defn exact-serialized-query
  "Return the canonical REST/app-DB representation used for Dima query identity."
  [query]
  (-> query
      lib.serialize/prepare-for-serialization
      json-wire-value))

(defn- absolute-datetime-clause?
  [value]
  (and (vector? value)
       (contains? #{:absolute-datetime "absolute-datetime"} (first value))))

(defn- hydrate-absolute-datetime!
  [clause]
  (when-not (= 4 (count clause))
    (fail! "NATIVE_QUERY_COMPAT_UNSUPPORTED_TEMPORAL_REPRESENTATION" 422
           "Dima compatibility supports only the native four-slot absolute-datetime clause"
           {:clause-tag (first clause)}))
  (let [literal (nth clause 2)]
    (when-not (and (string? literal)
                   (re-matches u.time/local-datetime-regex literal))
      (fail! "NATIVE_QUERY_COMPAT_UNSUPPORTED_TEMPORAL_REPRESENTATION" 422
             "Dima compatibility supports only local ISO datetime strings in absolute-datetime literal slots"
             {:clause-tag (first clause)
              :literal-type (some-> literal class .getName)}))
    (let [temporal (u.time/coerce-to-timestamp literal {:local true})]
      (when-not (instance? LocalDateTime temporal)
        (fail! "NATIVE_QUERY_COMPAT_UNSUPPORTED_TEMPORAL_REPRESENTATION" 422
               "Native temporal utility did not produce LocalDateTime for absolute-datetime"
               {:clause-tag (first clause)}))
      (assoc clause 2 temporal))))

(defn hydrate-exact-serialized-query!
  "Hydrate only certified runtime representation slots in exact serialized pMBQL.

  This function never interprets dates, periods, filters, or business meaning. Any
  absolute-datetime representation outside the certified local ISO datetime shape fails closed."
  [serialized-query]
  (when-not (map? serialized-query)
    (fail! "NATIVE_QUERY_COMPAT_INVALID_ARTIFACT" 409
           "Exact serialized pMBQL must be a map"
           nil))
  (walk/postwalk
   (fn [value]
     (if (absolute-datetime-clause? value)
       (hydrate-absolute-datetime! value)
       value))
   serialized-query))

(defn restore-exact-query!
  "Restore A to executable native runtime representation H(A) and prove reversibility."
  [serialized-query]
  (let [exact       (exact-serialized-query serialized-query)
        database-id (or (:database exact) (get exact "database"))]
    (when-not (pos-int? database-id)
      (fail! "NATIVE_QUERY_PRODUCER_INVALID" 409
             "Persisted native query has no positive database id"
             nil))
    (let [;; A is JSON/app-DB wire form. Native Lib normalization restores MBQL5
          ;; keys/tags/options without invoking lib/query's metadata/type enrichment. Then
          ;; native post-deserialization stripping runs exactly as for persisted/API queries.
          internal      (-> (lib.normalize/normalize
                              ::lib.schema/query
                              exact
                              {:throw? true})
                            lib.serialize/prepare-after-deserialization)
          ;; Pinned 0.63.18 intentionally leaves the absolute-datetime literal as a string.
          ;; This one certified representation slot is the only Dima-owned compatibility step.
          hydrated      (hydrate-exact-serialized-query! internal)
          ;; Attach only QP-internal metadata. prepare-for-serialization removes it, so this
          ;; cannot become part of canonical artifact identity.
          query         (assoc hydrated
                               :lib/metadata
                               (lib-be/application-database-metadata-provider database-id))
          roundtrip     (exact-serialized-query query)]
      (when-not (= exact roundtrip)
        (fail! "NATIVE_QUERY_COMPAT_NON_REVERSIBLE" 409
               "Hydrated native query does not serialize back to the exact authorized pMBQL artifact"
               nil))
      query)))
