(ns metabase.dima.native-execution
  "Exact-occurrence execution adapter into native Metabase Query Processor.

  This namespace is not an analytical engine or authorization authority. It re-attests the
  server-side Metabot occurrence under the current subject, binds it to Platform's expected
  fingerprint/attestation identity, restores reversible runtime representation, and delegates
  execution to native Metabase QP."
  (:require
   [clojure.string :as str]
   [metabase.api.common :as api]
   [metabase.dima.native-attestation :as dima.attestation]
   [metabase.dima.native-query-compat :as dima.compat]
   [metabase.query-processor :as qp])
  (:import
   (java.util UUID)))

(set! *warn-on-reflection* true)

(defn- fail!
  [code status message data]
  (throw (ex-info message
                  (merge {:status-code status
                          :dima/error-code code}
                         data))))

(defn execute-native-query!
  "Execute one exact native Metabot occurrence after identity binding.

  Caller input is locator + expected identities only. No caller-supplied query body is accepted."
  [{:keys [conversation_id native_query_id
           expected_pmbql_fingerprint expected_attestation_id]}]
  (when-not (instance? UUID conversation_id)
    (fail! "NATIVE_QUERY_OCCURRENCE_INVALID_LOCATOR" 400
           "conversation_id must be a UUID"
           nil))
  (when (str/blank? native_query_id)
    (fail! "NATIVE_QUERY_OCCURRENCE_INVALID_LOCATOR" 400
           "native_query_id must be non-empty"
           nil))
  (when-not (re-matches #"[0-9a-f]{64}" (or expected_pmbql_fingerprint ""))
    (fail! "NATIVE_EXECUTION_EXPECTATION_INVALID" 400
           "expected_pmbql_fingerprint must be an exact SHA-256 hex digest"
           nil))
  (when (str/blank? expected_attestation_id)
    (fail! "NATIVE_EXECUTION_EXPECTATION_INVALID" 400
           "expected_attestation_id must be non-empty"
           nil))
  (let [attestation (dima.attestation/attest-native-query!
                     {:conversation_id conversation_id
                      :native_query_id native_query_id})
        manifest    (:manifest attestation)
        actual-fp   (:exact_pmbql_fingerprint manifest)
        actual-att  (:attestation_id manifest)]
    (when-not (= expected_pmbql_fingerprint actual-fp)
      (fail! "NATIVE_EXECUTION_FINGERPRINT_MISMATCH" 409
             "Current native query occurrence does not match the authorized exact pMBQL fingerprint"
             {:expected_pmbql_fingerprint expected_pmbql_fingerprint
              :observed_pmbql_fingerprint actual-fp}))
    (when-not (= expected_attestation_id actual-att)
      (fail! "NATIVE_EXECUTION_ATTESTATION_MISMATCH" 409
             "Current native query occurrence does not match the authorized attestation identity"
             {:expected_attestation_id expected_attestation_id
              :observed_attestation_id actual-att}))
    (let [query      (dima.compat/restore-exact-query! (:exact_serialized_pmbql attestation))
          restored-fp (dima.attestation/exact-query-fingerprint query)]
      (when-not (= actual-fp restored-fp)
        (fail! "NATIVE_EXECUTION_FINGERPRINT_MISMATCH" 409
               "Hydrated execution representation does not preserve the authorized exact pMBQL fingerprint"
               {:expected_pmbql_fingerprint actual-fp
                :observed_pmbql_fingerprint restored-fp}))
      (let [result (qp/process-query
                    (qp/userland-query-with-default-constraints
                     query
                     {:executed-by api/*current-user-id*
                      :context :ad-hoc}))]
        (when-not (= :completed (:status result))
          (fail! "NATIVE_EXECUTION_QP_FAILURE" 422
                 "Native Metabase Query Processor did not complete the exact occurrence"
                 {:qp-status (:status result)
                  :qp-error (some-> (:error result) str)}))
        {:native_conversation_id (str conversation_id)
         :native_query_id native_query_id
         :attestation_id actual-att
         :executed_exact_pmbql_fingerprint actual-fp
         :runtime_identity (:runtime_identity manifest)
         :result result}))))
