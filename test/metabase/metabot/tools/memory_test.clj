(ns metabase.metabot.tools.memory-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [metabase.metabot.tools.memory :as tools.memory]
   [metabase.test :as mt]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

;; Notes are a shared/global store, so these tests write real rows. Wrapping the mutating body in a
;; rollback-only transaction keeps the table clean and the tests re-runnable in one REPL/JVM.

(deftest write-and-read-note-test
  (mt/with-temp [:model/User {user-id :id} {}]
    (mt/with-current-user user-id
      (t2/with-transaction [_conn nil {:rollback-only true}]
        (testing "write_note saves a note stamped with the current user"
          (let [{:keys [output structured-output]}
                (tools.memory/write-note-tool {:key "orders-status" :summary "status codes"
                                               :content "1=new 2=paid"})]
            (is (str/includes? output "orders-status"))
            (is (= {:note-key "orders-status"} structured-output))
            (is (=? {:summary "status codes" :content "1=new 2=paid" :creator_id user-id}
                    (t2/select-one :model/MetabotNote :note_key "orders-status")))))
        (testing "writing the same key overwrites rather than duplicating"
          (tools.memory/write-note-tool {:key "orders-status" :summary "status codes v2"
                                         :content "1=new 2=paid 3=shipped"})
          (is (= 1 (t2/count :model/MetabotNote :note_key "orders-status")))
          (is (= "1=new 2=paid 3=shipped"
                 (t2/select-one-fn :content :model/MetabotNote :note_key "orders-status"))))
        (testing "read_note returns full bodies and flags keys with no note"
          (let [{:keys [output structured-output]}
                (tools.memory/read-note-tool {:keys ["orders-status" "does-not-exist"]})]
            (is (str/includes? output "1=new 2=paid 3=shipped"))
            (is (str/includes? output "no note saved"))
            (is (= ["orders-status"] (:found structured-output)))))))))

(deftest list-notes-test
  (mt/with-temp [:model/User {user-id :id} {}]
    (mt/with-current-user user-id
      (t2/with-transaction [_conn nil {:rollback-only true}]
        (tools.memory/write-note-tool {:key "trustworthy-tables" :summary "orders clean; raw_events not"
                                       :content "..."})
        (let [{:keys [output structured-output]} (tools.memory/list-note-tool {})]
          (is (str/includes? output "trustworthy-tables — orders clean; raw_events not"))
          (is (pos? (:count structured-output))))))))

(deftest delete-note-test
  (mt/with-temp [:model/User {user-id :id} {}]
    (mt/with-current-user user-id
      (t2/with-transaction [_conn nil {:rollback-only true}]
        (tools.memory/write-note-tool {:key "temp-note" :summary "s" :content "c"})
        (testing "delete_note removes an existing note"
          (let [{:keys [output]} (tools.memory/delete-note-tool {:key "temp-note"})]
            (is (str/includes? output "Deleted"))
            (is (zero? (t2/count :model/MetabotNote :note_key "temp-note")))))
        (testing "delete_note on a missing key reports nothing to delete"
          (let [{:keys [output]} (tools.memory/delete-note-tool {:key "temp-note"})]
            (is (str/includes? output "No note"))))))))

(deftest system-context-test
  (testing "the memory section always carries the capability hint"
    (let [{:keys [megabot_notes]} (tools.memory/megabot-notes-system-context {})]
      (is (str/includes? megabot_notes "## Memory"))
      (is (str/includes? megabot_notes "write_note"))))
  (mt/with-temp [:model/User {user-id :id} {}]
    (mt/with-current-user user-id
      (t2/with-transaction [_conn nil {:rollback-only true}]
        (tools.memory/write-note-tool {:key "arr-definition" :summary "how ARR is computed here"
                                       :content "sum(active plan monthly price) * 12"})
        (testing "saved notes appear in the injected catalog by key + summary"
          (let [{:keys [megabot_notes]} (tools.memory/megabot-notes-system-context {})]
            (is (str/includes? megabot_notes "arr-definition — how ARR is computed here"))))))))
