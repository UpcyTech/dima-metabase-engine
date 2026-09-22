(ns metabase.metabot.tools.memory
  "Persistent-notes tools and system-prompt injection for the experimental `:megabot` profile.

  Megabot keeps durable instance facts as notes it reads and writes across conversations — a schema
  quirk, which tables are trustworthy, how a metric is defined here, a piece of reusable SQL. The
  agent sees a catalog of note keys + one-line summaries in its system prompt (injected via
  `megabot-notes-system-context`) and pulls a note's full body on demand with `read_note`, mirroring
  the skills catalog + `load_skill` pattern. Notes are shared across the instance; `write_note` stamps
  the current user as the note's author. Storage goes through `metabase.metabot.db`."
  (:require
   [clojure.string :as str]
   [metabase.api.common :as api]
   [metabase.metabot.db :as metabot.db]
   [metabase.util.malli :as mu]))

(set! *warn-on-reflection* true)

(def ^:private max-notes-in-catalog
  "Cap on how many note lines are listed in the always-on system-prompt catalog."
  200)

(defn- catalog-lines
  "Render `catalog` rows (maps with :note_key/:summary) as `- <key> — <summary>` markdown lines."
  [catalog]
  (->> catalog
       (map (fn [{:keys [note_key summary]}]
              (str "- " note_key " — " summary)))
       (str/join "\n")))

;;; ------------------------------------------------ System-prompt hook ------------------------------------------------

(defn megabot-notes-system-context
  "Profile `:system-prompt-context` hook: returns `{:megabot_notes <section>}` rendered into
  `megabot.selmer`. Emits a `## Memory` section listing the note catalog (keys + summaries) so durable
  instance facts are always in front of the agent; full bodies are pulled on demand with `read_note`.
  Always emits the capability hint, even with no notes, so the agent knows it can start remembering."
  [_context]
  (let [catalog (take max-notes-in-catalog (metabot.db/note-catalog))
        header  (str "## Memory\n"
                     "You keep persistent notes across conversations. Save a durable fact with "
                     "`write_note`, update or remove one with `write_note`/`delete_note`, and read a "
                     "note's full body with `read_note`.")]
    {:megabot_notes
     (if (empty? catalog)
       (str header "\n\nNo notes saved yet.")
       (str header "\n\nSaved notes (read the ones you need by key):\n" (catalog-lines catalog)))}))

;;; ---------------------------------------------------- Tools ----------------------------------------------------

(mu/defn ^{:tool-name "write_note"}
  write-note-tool
  "Save a durable fact to persistent memory so future conversations start with it instead of
  rediscovering it — a schema quirk, which tables are trustworthy, how a metric is defined here, a
  piece of reusable SQL. `key` is a short, stable slug that identifies the note (e.g.
  \"orders-status-codes\"); writing the same key again overwrites that note. `summary` is a one-line
  description shown in the memory catalog. `content` is the full note body (markdown)."
  [{:keys [key summary content]}
   :- [:map {:closed true}
       [:key :string]
       [:summary :string]
       [:content :string]]]
  (try
    (metabot.db/upsert-note! key summary content api/*current-user-id*)
    {:output            (str "Saved note '" key "'.")
     :structured-output {:note-key key}}
    (catch Exception e
      {:output (str "Could not save note: " (ex-message e))})))

(mu/defn ^{:tool-name "read_note"}
  read-note-tool
  "Read the full body of one or more saved notes by key. `keys` is a list of note keys as shown in the
  memory catalog. Returns each note's content (and flags any key with no note saved under it)."
  [{note-keys :keys}
   :- [:map {:closed true}
       [:keys [:sequential :string]]]]
  (let [rows  (metabot.db/note-bodies-by-keys note-keys)
        found (into {} (map (juxt :note_key :content)) rows)
        parts (for [k note-keys]
                (if-let [content (get found k)]
                  (str "<note key=\"" k "\">\n" content "\n</note>")
                  (str "<note key=\"" k "\"> (no note saved under this key) </note>")))]
    {:output            (str/join "\n\n" parts)
     :structured-output {:found (mapv :note_key rows)}}))

(mu/defn ^{:tool-name "list_notes"}
  list-note-tool
  "List every saved note by key with its one-line summary — the live memory catalog, including notes
  written earlier in this same conversation. Read a note's full body with `read_note`."
  [_args :- [:map {:closed true}]]
  (let [catalog (metabot.db/note-catalog)]
    {:output            (if (empty? catalog)
                          "No notes saved yet."
                          (catalog-lines catalog))
     :structured-output {:count (count catalog)}}))

(mu/defn ^{:tool-name "delete_note"}
  delete-note-tool
  "Delete a saved note by key when it is wrong or no longer useful. `key` is the note's key as shown in
  the memory catalog."
  [{:keys [key]}
   :- [:map {:closed true}
       [:key :string]]]
  (let [n (metabot.db/delete-note! key)]
    {:output (if (pos? (or n 0))
               (str "Deleted note '" key "'.")
               (str "No note saved under key '" key "'."))}))
