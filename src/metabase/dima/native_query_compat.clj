(ns metabase.dima.native-query-compat
  "Dima-local runtime representation compatibility for exact native pMBQL artifacts.

  This namespace does not interpret analytical semantics. It only restores the JVM
  representation required by the pinned Metabase runtime and proves lossless round-trip
  back to the exact serialized authority."
  (:require
   [clojure.walk :as walk]
   [metabase.lib-be.core :as lib-be]
   [metabase.lib.core :as lib]
   [metabase.lib.serialize :as lib.serialize]
   [metabase.util.json :as json]
   [metabase.util.time :as u.time])
  (:import
   (java.time.temporal Temporal)))

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
  "Return the canonical REST/app-DB wire artifact A for one pMBQL query."
  [query]
  (-> query
      lib.serialize/prepare-for-serialization
      json-wire-value))

(defn- absolute-datetime-tag? [x]
  (or (= x :absolute-datetime)
      (= x "absolute-datetime")))

(defn- hydrate-absolute-datetime-clause [clause]
  (if-not (and (vector? clause)
               (= 4 (count clause))
               (absolute-datetime-tag? (first clause)))
    clause
    (let [value (nth clause 2)]
      (cond
        (instance? Temporal value)
        clause

        (string? value)
        (let [hydrated (try
                         (u.time/coerce-to-timestamp value {:local true})
                         (catch Exception _ nil))]
          (when-not (instance? java.time.LocalDateTime hydrated)
            (fail! "NATIVE_QUERY_RUNTIME_REPRESENTATION_UNSUPPORTED" 422
                   "Dima compatibility codec supports only local ISO datetime strings in :absolute-datetime literal slots"
                   {:clause-tag "absolute-datetime"}))
          (assoc clause 2 hydrated))

        :else
        (fail! "NATIVE_QUERY_RUNTIME_REPRESENTATION_UNSUPPORTED" 422
               "Dima compatibility codec encountered an unsupported :absolute-datetime runtime representation"
               {:clause-tag "absolute-datetime"
                :value-type (some-> value class .getName)})))))

(defn hydrate-runtime-query!
  "Hydrate only certified runtime representation defects and prove serialize(H(A)) == A."
  [query]
  (let [authority (exact-serialized-query query)
        hydrated  (walk/postwalk hydrate-absolute-datetime-clause query)
        roundtrip (exact-serialized-query hydrated)]
    (when-not (= authority roundtrip)
      (fail! "NATIVE_QUERY_RUNTIME_ROUNDTRIP_MISMATCH" 409
             "Hydrated runtime query does not serialize back to the exact authoritative pMBQL artifact"
             nil))
    hydrated))

(defn restore-exact-runtime-query!
  "Restore exact serialized authority A into a lossless native runtime representation H(A)."
  [exact-serialized-pmbql]
  (let [database-id (or (:database exact-serialized-pmbql)
                        (get exact-serialized-pmbql "database"))]
    (when-not (pos-int? database-id)
      (fail! "NATIVE_QUERY_PRODUCER_INVALID" 409
             "Exact serialized pMBQL has no positive database id"
             nil))
    (let [restored (lib/query
                    (lib-be/application-database-metadata-provider database-id)
                    exact-serialized-pmbql)]
      (when-not (= exact-serialized-pmbql (exact-serialized-query restored))
        (fail! "NATIVE_QUERY_RUNTIME_ROUNDTRIP_MISMATCH" 409
               "Native Lib restore changed the authoritative serialized pMBQL artifact"
               nil))
      (hydrate-runtime-query! restored))))
