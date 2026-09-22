---
id: megabot-discovery
title: Finding database, table, and field ids
description: How to look up the numeric ids and schema you need before running a warehouse query.
profiles: [megabot]
---
Before you can run a warehouse query you need the numeric `database_id` and the table/column names.
Metabase keeps all of that in its own application database — look it up with `query_app_db`:

- Databases: `SELECT id, name, engine FROM metabase_database`
- Tables in a database: `SELECT id, name, schema FROM metabase_table WHERE db_id = <database id> AND active = true`
- Columns in a table: `SELECT id, name, base_type FROM metabase_field WHERE table_id = <table id> AND active = true`

Then use those ids: pass the `database_id` to `run_warehouse_sql`, or the numeric `source-table` and
field ids inside the MBQL you give to `run_warehouse_query`.
