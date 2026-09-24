(ns metabase.dima.native-execution
  "Exact-occurrence execution adapter into the native Metabase Query Processor.

  Dima supplies no analytical plan here. The adapter reloads and re-attests the
  persisted Metabot occurrence, binds it to the already-authorized exact fingerprint,
  hydrates only runtime representation, and delegates execution to native QP."
  (:require
   [clojure.string :as str]
   [metabase.dima.native-attestation :as dima.attestation]
   [metabase.dima.native-query-compat :as dima.compat]
   [metabase.query-processor :as qp])
  (:import
   (java.util UUID)))

(set! *warn-on-reflection* true)

(defn- fail!
  ([code status message]
   (fail! code status message nil))
  ([code status message data]
   (throw (ex-info message
                   (merge {:status-code status
                           :dima/error-code code}
                          data)))))

(defn execute-native-query!
  "Execute one exact server-side Metabot occurrence after identity binding.

  Inputs are locator/identity only. No caller-supplied query is accepted."
  [{:keys [conversation_id
           native_query_id
           expected_pmbql_fingerprint
           expected_attestation_id]}]
  (when-not (instance? UUID conversation_id)
    (fail! "NATIVE_QUERY_OCCURRENCE_INVALID_LOCATOR" 400
           "conversation_id must be a UUID"))
  (when (str/blank? native_query_id)
    (fail! "NATIVE_QUERY_OCCURRENCE_INVALID_LOCATOR" 400
           "native_query_id must be non-empty"))
  (when (str/blank? expected_pmbql_fingerprint)
    (fail! "NATIVE_QUERY_EXECUTION_IDENTITY_INVALID" 400
           "expected_pmbql_fingerprint is required"))
  (when (str/blank? expected_attestation_id)
    (fail! "NATIVE_QUERY_EXECUTION_IDENTITY_INVALID" 400
           "expected_attestation_id is required"))
  (let [{:keys [exact_serialized_pmbql manifest] :as attestation}
        (dima.attestation/attest-native-query!
         {:conversation_id conversation_id
          :native_query_id native_query_id})
        observed-fingerprint (:exact_pmbql_fingerprint manifest)
        observed-attestation (:attestation_id manifest)]
    (when-not (= expected_pmbql_fingerprint observed-fingerprint)
      (fail! "NATIVE_QUERY_EXECUTION_FINGERPRINT_MISMATCH" 409
             "Current exact query occurrence fingerprint differs from the authorized fingerprint"
             {:expected_pmbql_fingerprint expected_pmbql_fingerprint
              :observed_pmbql_fingerprint observed-fingerprint}))
    (when-not (= expected_attestation_id observed-attestation)
      (fail! "NATIVE_QUERY_EXECUTION_ATTESTATION_MISMATCH" 409
             "Current exact query occurrence attestation differs from the authorized attestation"
             {:expected_attestation_id expected_attestation_id
              :observed_attestation_id observed-attestation}))
    (let [runtime-query (dima.compat/restore-exact-runtime-query! exact_serialized_pmbql)
          result        (qp/process-query
                         (qp/userland-query-with-default-constraints runtime-query))
          runtime       (dima.attestation/runtime-identity)]
      (when-not (= runtime (:runtime_identity manifest))
        (fail! "NATIVE_QUERY_EXECUTION_RUNTIME_MISMATCH" 409
               "Runtime identity changed between attestation and exact-occurrence execution"))
      (when-not (= :completed (:status result))
        (fail! "NATIVE_QUERY_EXECUTION_FAILED" 422
               "Native Metabase Query Processor did not complete the exact occurrence"
               {:query-status (:status result)}))
      {:native_conversation_id (str conversation_id)
       :native_query_id native_query_id
       :attestation_id observed-attestation
       :executed_pmbql_fingerprint observed-fingerprint
       :runtime_identity runtime
       :result result
       :attestation attestation})))
