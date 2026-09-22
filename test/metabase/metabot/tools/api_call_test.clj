(ns metabase.metabot.tools.api-call-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [metabase.metabot.tools.api-call :as api-call]
   [metabase.test :as mt]))

(defn- call [args]
  (api-call/call-api-tool args))

(defn- extract-id
  "Pull the first `\"id\":<n>` out of a call_api :output string."
  [output]
  (some-> (re-find #"\"id\":(\d+)" output) second Long/parseLong))

;; call_api dispatches through the real production handler, which returns 503 until initialization is
;; complete. These tests exercise the tool directly (not via the test HTTP client, which would do this
;; itself), so nothing else completes init — initialize the :web-server component, which marks it complete.
(use-fixtures :once (fn [thunk]
                      (mt/initialize-if-needed! :web-server)
                      (thunk)))

;;; ---------------------------------------------- call_api ----------------------------------------------

(deftest call-api-runs-as-current-user-test
  (testing "call_api dispatches as the bound user (not as a superuser)"
    (mt/with-test-user :rasta
      (let [{:keys [output structured-output]} (call {:method "GET" :path "/api/user/current"})]
        (is (str/starts-with? output "HTTP 200"))
        (is (str/includes? output (str "\"id\":" (mt/user->id :rasta)))
            "the /api/user/current response is rasta, proving the request ran as rasta")
        (is (= "/api/user/current" (:path structured-output)))
        (is (= 200 (:status structured-output)))))))

(deftest call-api-write-then-read-test
  (testing "a POST write succeeds and is visible via a follow-up GET"
    (mt/with-model-cleanup [:model/Collection]
      (mt/with-test-user :crowberto
        (let [{:keys [output]} (call {:method "POST"
                                      :path   "/api/collection"
                                      :body   {:name "Megabot API QA"}})]
          (is (str/starts-with? output "HTTP 20"))
          (let [id (extract-id output)]
            (is (int? id))
            (let [{:keys [output]} (call {:method "GET" :path (str "/api/collection/" id)})]
              (is (str/starts-with? output "HTTP 200"))
              (is (str/includes? output "Megabot API QA")))))))))

(deftest call-api-enforces-permissions-test
  (testing "a call the user isn't allowed to make returns the API's own 4xx, not a bypass"
    (mt/with-test-user :rasta
      (let [{:keys [output structured-output]} (call {:method "GET" :path "/api/setting"})]
        (is (re-find #"HTTP 40[13]" output)
            "rasta is not an admin, so listing settings must be forbidden")
        (is (contains? #{401 403} (:status structured-output)))))))

(deftest call-api-normalizes-path-test
  (testing "the /api prefix is added and hosts/leading-slash variations all resolve"
    (mt/with-test-user :rasta
      (doseq [p ["/api/user/current" "/user/current" "user/current"]]
        (let [{:keys [output]} (call {:method "GET" :path p})]
          (is (str/starts-with? output "HTTP 200") (str "path variant: " p)))))))

(deftest call-api-query-params-test
  (testing "query_params are passed through to the handler (request succeeds with a filter param)"
    (mt/with-test-user :crowberto
      (let [{:keys [output]} (call {:method       "GET"
                                    :path         "/api/collection"
                                    :query_params {:archived true}})]
        (is (str/starts-with? output "HTTP 200"))))))

(deftest call-api-streaming-response-test
  (testing "a streaming query-execution response is realized into :output"
    (mt/with-test-user :crowberto
      (let [{:keys [output]} (call {:method "POST"
                                    :path   "/api/dataset"
                                    :body   {:database (mt/id)
                                             :type     "native"
                                             :native   {:query "SELECT 1 AS n"}}})]
        (is (str/starts-with? output "HTTP 20"))
        (is (str/includes? output "\"rows\"")
            "the realized streaming body should contain the query result rows")))))

(deftest call-api-read-only-setting-test
  (testing "megabot-api-read-only? refuses non-GET but still allows GET"
    (mt/with-temporary-setting-values [megabot-api-read-only? true]
      (mt/with-test-user :crowberto
        (let [{:keys [output]} (call {:method "POST" :path "/api/collection" :body {:name "nope"}})]
          (is (str/includes? output "read-only")))
        (let [{:keys [output]} (call {:method "GET" :path "/api/user/current"})]
          (is (str/starts-with? output "HTTP 200")))))))

(deftest call-api-error-path-test
  (testing "an unknown path returns an HTTP status string, never throws"
    (mt/with-test-user :rasta
      (let [{:keys [output]} (call {:method "GET" :path "/api/definitely-not-a-real-endpoint"})]
        (is (string? output))
        (is (re-find #"HTTP \d\d\d" output))))))

;;; ---------------------------------- list_api_endpoints / describe ----------------------------------

(deftest list-api-endpoints-test
  (testing "no args returns a paged index"
    (let [{:keys [output]} (api-call/list-api-endpoints-tool {})]
      (is (str/includes? output "Matched"))
      (is (str/includes? output "/api/"))))
  (testing "search narrows the results"
    (let [{:keys [output]} (api-call/list-api-endpoints-tool {:search "collection"})]
      (is (str/includes? (str/lower-case output) "collection"))))
  (testing "method filter narrows to one verb"
    (let [{:keys [output]} (api-call/list-api-endpoints-tool {:method "POST" :search "collection"})]
      (is (str/includes? output "POST"))
      (is (not (str/includes? output "\nGET ")))))
  (testing "paging is honored"
    (let [{:keys [output]} (api-call/list-api-endpoints-tool {:page 2 :page_size 5})]
      (is (str/includes? output "Page 2/")))))

(deftest describe-api-endpoint-test
  (testing "returns the operation schema for a known endpoint"
    (let [{:keys [output]} (api-call/describe-api-endpoint-tool {:path "/api/collection" :method "POST"})]
      (is (str/includes? output "Endpoint: /api/collection"))
      (is (str/includes? output "Operation"))))
  (testing "a templated id path resolves via both {id} and :id forms"
    (doseq [p ["/api/collection/{id}" "/api/collection/:id"]]
      (let [{:keys [output]} (api-call/describe-api-endpoint-tool {:path p})]
        (is (not (str/includes? output "No endpoint found")) (str "path variant: " p)))))
  (testing "an unknown path returns a friendly message"
    (let [{:keys [output]} (api-call/describe-api-endpoint-tool {:path "/api/definitely-not-real"})]
      (is (str/includes? output "No endpoint found")))))
