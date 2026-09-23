(ns metabase.dima.api
  "Isolated Dima engine trust endpoints.

  These endpoints expose engine identity and bounded observation of already-produced native
  Metabot queries. They contain no analytical planning or business-semantic authority."
  (:require
   [metabase.api.common :as api]
   [metabase.api.macros :as api.macros]
   [metabase.dima.native-attestation :as dima.attestation]
   [metabase.util.malli.schema :as ms])
  (:import
   (java.util UUID)))

(set! *warn-on-reflection* true)

(def ^:private RuntimeIdentityResponse
  [:map
   [:repository ms/NonBlankString]
   [:revision_sha [:re #"^[0-9a-f]{40}$"]]
   [:upstream_base_sha [:re #"^[0-9a-f]{40}$"]]
   [:runtime_tag ms/NonBlankString]
   [:build_identity ms/NonBlankString]
   [:image_identity ms/NonBlankString]
   [:runtime_instance_id ms/UUIDString]])

(def ^:private AggregationFact
  [:map
   [:operator ms/NonBlankString]
   [:argument_kind ms/NonBlankString]
   [:referenced_field_ids [:sequential :int]]
   [:distinct :boolean]])

(def ^:private TemporalPredicate
  [:map
   [:time_field_id :int]
   [:operator ms/NonBlankString]
   [:lower_bound [:maybe :string]]
   [:upper_bound [:maybe :string]]
   [:lower_inclusive [:maybe :boolean]]
   [:upper_inclusive [:maybe :boolean]]
   [:field_temporal_type ms/NonBlankString]
   [:temporal_unit [:maybe :string]]])

(def ^:private TextualEqualityPredicate
  [:map
   [:stage_number ms/IntGreaterThanOrEqualToZero]
   [:field_id ms/PositiveInt]
   [:operator [:= "="]]
   [:literal_value :string]
   [:field_type ms/NonBlankString]])

(def ^:private ValidationProvenance
  [:map
   [:producer_structured_output [:= "PASSED"]]
   [:pmbql_schema [:= "PASSED"]]
   [:producer_query_id_match [:= "PASSED"]]
   [:producer_state_match [:= "PASSED"]]])

(def ^:private PermissionProvenance
  [:map
   [:current_metabase_user_id ms/PositiveInt]
   [:permission_check [:= "PASSED"]]
   [:checked_source_table_ids [:sequential :int]]])

(def ^:private NativeExecutionManifest
  [:map
   [:attestation_id ms/NonBlankString]
   [:native_conversation_id ms/UUIDString]
   [:native_assistant_message_id ms/PositiveInt]
   [:native_tool_call_id ms/NonBlankString]
   [:native_query_id ms/NonBlankString]
   [:producer_tool [:= "construct_notebook_query"]]
   [:exact_pmbql_fingerprint [:re #"^[0-9a-f]{64}$"]]
   [:database_id ms/PositiveInt]
   [:primary_source_table_id [:maybe ms/PositiveInt]]
   [:referenced_source_table_ids [:sequential ms/PositiveInt]]
   [:aggregation_count ms/IntGreaterThanOrEqualToZero]
   [:aggregations [:sequential AggregationFact]]
   [:breakout_count ms/IntGreaterThanOrEqualToZero]
   [:material_filter_count ms/IntGreaterThanOrEqualToZero]
   [:non_temporal_filter_count ms/IntGreaterThanOrEqualToZero]
   [:temporal_predicates [:sequential TemporalPredicate]]
   [:textual_equality_predicates [:sequential TextualEqualityPredicate]]
   [:explicit_join_count ms/IntGreaterThanOrEqualToZero]
   [:implicit_join_count ms/IntGreaterThanOrEqualToZero]
   [:implicit_joined_table_ids [:sequential ms/PositiveInt]]
   [:order_by_count ms/IntGreaterThanOrEqualToZero]
   [:limit [:maybe ms/IntGreaterThanOrEqualToZero]]
   [:stage_count ms/IntGreaterThanOrEqualToZero]
   [:material_query_count ms/IntGreaterThanOrEqualToZero]
   [:authenticated_metabase_subject ms/PositiveInt]
   [:validation_provenance ValidationProvenance]
   [:permission_provenance PermissionProvenance]
   [:runtime_identity RuntimeIdentityResponse]])

(def ^:private NativeQueryAttestationResponse
  [:map
   [:exact_serialized_pmbql :map]
   [:manifest NativeExecutionManifest]])

(defn- check-no-dropped-entries!
  "Fail closed when endpoint decoding dropped an undeclared or invalid map entry.

  The pinned v0.63.18 base predates upstream api/check-no-dropped-entries, so this
  Dima-local compatibility shim preserves that native Metabase helper's exact
  count-based semantics without modifying a second upstream-owned source file."
  [raw decoded]
  (api/check-400
   (or (not (map? raw))
       (= (count raw) (count decoded)))
   "The request contains keys or values this endpoint does not accept."))

(api.macros/defendpoint :get "/engine/v1/identity" :- RuntimeIdentityResponse
  "Return immutable Dima engine build/runtime identity. Contains no business or query logic."
  []
  (dima.attestation/runtime-identity))

(api.macros/defendpoint :post "/engine/v1/native-query-attestation" :- NativeQueryAttestationResponse
  "Attest one already-produced native Metabot query by server-side occurrence identity only."
  [_route-params
   _query-params
   body
   :- [:map {:closed true}
       [:conversation_id ms/UUIDString]
       [:native_query_id ms/NonBlankString]]
   request]
  (check-no-dropped-entries! (:body request) body)
  (let [{:keys [conversation_id native_query_id]} body]
    (dima.attestation/attest-native-query!
     {:conversation_id (UUID/fromString conversation_id)
      :native_query_id native_query_id})))

(def keep-me
  "Require target for the root API router."
  true)
