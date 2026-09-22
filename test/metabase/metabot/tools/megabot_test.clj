(ns metabase.metabot.tools.megabot-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [metabase.app-db.core :as mdb]
   [metabase.metabot.agent.links :as links]
   [metabase.metabot.db :as metabot.db]
   [metabase.metabot.tools.megabot :as megabot]
   [metabase.metabot.tools.shared :as shared]
   [metabase.test :as mt]))

(defn- resolves-to-question-url?
  "Whether `uri` (a `metabase://query/…` or `metabase://chart/…` link) resolves against `memory`'s
  state to an ad-hoc `/question#` URL."
  [memory uri]
  (str/starts-with? (links/resolve-links (str "[x](" uri ")")
                                         (get-in @memory [:state :queries])
                                         (get-in @memory [:state :charts])
                                         (atom {}))
                    "[x](/question#"))

(deftest query-app-db-refuses-writes-test
  (testing "query_app_db refuses any statement that is not a read"
    (doseq [sql ["delete from setting"
                 "DROP TABLE setting"
                 "update core_user set is_superuser = true"
                 "insert into setting (key, value) values ('x', 'y')"]]
      (let [{:keys [output structured-output]} (megabot/query-app-db-tool {:sql sql})]
        (is (str/includes? output "read-only")
            (str "should refuse: " sql))
        (is (nil? structured-output))))))

(deftest query-app-db-reads-test
  (testing "query_app_db runs a SELECT against the app db and returns rows"
    (let [{:keys [output structured-output]} (megabot/query-app-db-tool {:sql "select 1 as n"})]
      (is (some? output))
      (is (= 1 (:row-count structured-output)))))
  (testing "row_limit caps returned rows"
    (let [{:keys [output structured-output]} (megabot/query-app-db-tool
                                              {:sql "select id from metabase_database" :row_limit 1})]
      (is (some? structured-output) (str "expected rows, got output: " output))
      (is (<= (:row-count structured-output) 1)))))

(deftest app-db-write-is-rolled-back-test
  ;; H2 only: the write below uses H2/ANSI double-quoted reserved-word columns; the rollback mechanism
  ;; itself is dialect-independent (Metabase savepoints), so covering the default dev/test app db is enough.
  (when (= :h2 (mdb/db-type))
    (testing "a write reaching run-read-only-app-db-sql (past the tool's keyword guard) does not persist"
      (let [count-sql "select count(*) as c from setting"
            n-before  (:c (first (metabot.db/run-read-only-app-db-sql count-sql)))]
        (metabot.db/run-read-only-app-db-sql
         "insert into setting (\"KEY\", \"VALUE\") values ('megabot-rollback-probe', 'x')")
        (is (= n-before (:c (first (metabot.db/run-read-only-app-db-sql count-sql))))
            "the insert must have been rolled back, leaving the row count unchanged")))))

(deftest run-warehouse-sql-error-path-test
  (testing "an unrunnable query returns an error string in :output rather than throwing"
    (let [{:keys [output structured-output]} (megabot/run-warehouse-sql-tool {:database_id 0 :sql "select 1"})]
      (is (string? output))
      (is (or (str/includes? output "Query failed") (str/includes? output "Query error")))
      (testing "and the error carries a recovery hint so the model can fix the next call"
        (is (str/includes? output "To recover:"))
        (is (str/includes? output "query_app_db")))
      (testing "and registers nothing: no structured output means no query id"
        (is (nil? structured-output))))))

(deftest run-warehouse-sql-returns-rows-test
  (testing "a valid native query against a warehouse returns rows the LLM can read"
    (mt/test-drivers #{:h2}
      (let [memory (atom {:state {}})
            raw    {:database (mt/id) :type :native :native {:query "SELECT 1 AS n"}}
            {:keys [output structured-output]}
            (binding [shared/*memory-atom* memory]
              (megabot/run-warehouse-sql-tool {:database_id (mt/id) :sql "SELECT 1 AS n"}))
            query-id (:query-id structured-output)]
        (is (str/includes? output "1"))
        (is (= 1 (:row-count structured-output)))
        (testing "registers the raw legacy query under a fresh query id"
          (is (not (str/blank? query-id)))
          (is (= raw (:query structured-output)))
          (is (= (mt/id) (:database structured-output)))
          (is (str/includes? output (str "Query ID: " query-id)))
          (is (str/includes? output (str "metabase://query/" query-id))))
        (testing "the query is in memory immediately (live state and the persisted turn delta)"
          (is (= raw (get-in @memory [:state :queries query-id])))
          (is (= raw (get-in @memory [:turn-state :queries query-id])))
          (is (resolves-to-question-url? memory (str "metabase://query/" query-id))))))))

(deftest run-warehouse-query-returns-rows-test
  (testing "a structured MBQL query against a warehouse returns rows the LLM can read"
    (mt/test-drivers #{:h2}
      (let [memory (atom {:state {}})
            raw    (mt/mbql-query orders {:aggregation [[:count]]})
            {:keys [output structured-output]}
            (binding [shared/*memory-atom* memory]
              (megabot/run-warehouse-query-tool {:query raw}))
            query-id (:query-id structured-output)]
        (is (some? output))
        (is (= 1 (:row-count structured-output)))
        (testing "registers the raw MBQL query under a fresh query id"
          (is (not (str/blank? query-id)))
          (is (= raw (:query structured-output)))
          (is (= raw (get-in @memory [:state :queries query-id])))
          (is (resolves-to-question-url? memory (str "metabase://query/" query-id))))))))

(deftest run-tools-without-memory-test
  (testing "the run tools still work when no memory atom is bound (e.g. called outside the agent loop)"
    (mt/test-drivers #{:h2}
      (let [{:keys [structured-output]}
            (megabot/run-warehouse-sql-tool {:database_id (mt/id) :sql "SELECT 1 AS n"})]
        (is (not (str/blank? (:query-id structured-output))))))))

;;; show_result — no database needed: it only reads a query from memory and emits parts, and
;;; `->legacy-mbql` passes a non-MBQL-5 stub through unchanged (same setup as charts_test).

(def ^:private stub-query
  {:database 1 :type "query" :query {:source-table 1}})

(defn- show-result
  [args]
  (let [memory (atom {:state {:queries {"q-1" stub-query}}})
        result (binding [shared/*memory-atom* memory]
                 (megabot/show-result-tool (merge {:query_id "q-1" :title "Orders"} args)))]
    {:memory memory :result result}))

(deftest show-result-renders-table-by-default-test
  (let [{:keys [memory result]} (show-result {:description "All orders."})
        {:keys [output structured-output data-parts]} result
        chart-id (:chart-id structured-output)
        entity   (:data (first data-parts))]
    (testing "emits exactly one generated_entity card the frontend runs and renders"
      (is (= 1 (count data-parts)))
      (is (= "generated_entity" (:data-type (first data-parts))))
      (is (= "card" (:type entity)))
      (is (= chart-id (:id entity)))
      (is (= "table" (:display entity)))
      (is (= "Orders" (:title entity)))
      (is (= "All orders." (:description entity)))
      (is (= "q-1" (get-in entity [:query :id])))
      (is (= stub-query (get-in entity [:query :query]))))
    (testing "structured output carries what extract-charts needs, including the query"
      (is (string? chart-id))
      (is (= :chart (:result-type structured-output)))
      (is (= "q-1" (:query-id structured-output)))
      (is (= stub-query (:query structured-output)))
      (is (= :table (:chart-type structured-output))))
    (testing "tells the model how to link the rendered result"
      (is (str/includes? output (str "metabase://chart/" chart-id))))
    (testing "the chart is in memory immediately, in the state shape chart links resolve against"
      (let [chart (get-in @memory [:state :charts chart-id])]
        (is (= chart-id (:chart_id chart)))
        (is (= "q-1" (:query_id chart)))
        (is (= [stub-query] (:queries chart)))
        (is (= :table (get-in chart [:visualization_settings :chart_type]))))
      (is (resolves-to-question-url? memory (str "metabase://chart/" chart-id))))))

(deftest show-result-explicit-display-test
  (testing "an explicit display type flows through to the card and the chart state"
    (let [{:keys [memory result]} (show-result {:display "bar"})
          {:keys [structured-output data-parts]} result]
      (is (= "bar" (get-in data-parts [0 :data :display])))
      (is (= :bar (:chart-type structured-output)))
      (is (= :bar (get-in @memory [:state :charts (:chart-id structured-output)
                                   :visualization_settings :chart_type])))
      (testing "and no description key is emitted when none was given"
        (is (not (contains? (get-in data-parts [0 :data]) :description)))))))

(deftest show-result-unknown-query-test
  (testing "an unknown query id returns an error naming the available ids, with nothing to render"
    (let [{:keys [result]} (show-result {:query_id "nope"})
          {:keys [output structured-output data-parts]} result]
      (is (str/includes? output "nope"))
      (is (str/includes? output "q-1"))
      (is (nil? structured-output))
      (is (nil? data-parts)))))
