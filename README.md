# DWH Reconciliation Tool

Checks that data in destination databases matches the DWH source tables, and produces an
Excel + HTML report of every difference. Built for large tables: the data is never copied
between databases. Each database summarises its own data, the summaries are compared, and
rows are pulled only for the small slices that actually differ.

## How it works

| Level | What runs (inside each database)                                   | What it catches |
|-------|---------------------------------------------------------------------|-----------------|
| 1     | `COUNT(*)` per partition (e.g. per month)                           | Missing / extra rows, failed loads |
| 2     | Per partition: sums of number columns, null counts, text lengths and a character signature, min/max/sum of dates, distinct key count | Changed values, truncated text, wrong dates, duplicate keys |
| 3     | Only for partitions that failed: split into key buckets, compare bucket fingerprints, fetch just the differing buckets and compare row by row. Plus a few random buckets every run (spot check). | The exact keys and column values that differ |

A typical daily run on matching data is one aggregate query per table on each side. When a
partition of 5 lakh rows has one bad row, the tool fetches roughly 20,000 rows (one bucket)
instead of 5 lakh.

**Issue types in the report:** `MISSING_IN_TARGET`, `EXTRA_IN_TARGET`, `VALUE_MISMATCH`
(with column, source value, target value), `DUPLICATE_KEY_IN_TARGET` / `_IN_SOURCE`,
`MOVED_PARTITION` (the partition column itself changed, e.g. issue date moved to another month).

**Values are normalised before comparing**, so these do *not* count as differences:
trailing/leading spaces, empty string vs NULL (Oracle treats them the same), number
precision beyond the configured `scale`, the time part of `date` columns, and letter case
for columns marked `ignore_case`.

## Setup (one time)

1. Python 3.10 or newer.
2. Install packages:
   ```
   pip install -r requirements.txt
   ```
   plus the driver for your databases: `oracledb` (Oracle), `pyodbc` + Microsoft ODBC Driver
   17/18 (SQL Server), `psycopg2-binary` (PostgreSQL), `pymysql` (MySQL).
3. Ask for a **read-only** user on each database. The tool only runs `SELECT` statements
   and never changes source or destination data. The only database write is the optional
   `results_db` setting (off by default), which appends to its own `RECON_*` tables.
4. Open `recon_config.yaml` and replace every `xxxx` (each line's comment says what goes
   there). Connections are given as separate fields (`host`, `service_name`, `server`,
   `database`, `driver`, `user`); a full SQLAlchemy `url:` still works if you prefer.
   The tool stops with a clear message if any `xxxx` is left.
5. Put passwords in a file named `.env` in the same folder as the config (copy
   `.env.example`). One line per `password_env` name used in the config:
   ```
   DWH_PWD=xxxx
   MIS_PWD=xxxx
   ```
   Keep `.env` out of email, shared drives and Git, and restrict the folder to the
   service account that runs the tool. A real environment variable with the same name
   overrides the file. Use `--env-file path` to keep the file somewhere else.

## Running

```
python recon.py -c recon_config.yaml --check              # test every connection first
python recon.py -c recon_config.yaml                      # all enabled tables
python recon.py -c recon_config.yaml -t policy_master     # one table (repeat -t for more)
python recon.py -c recon_config.yaml -l 1                 # counts only (fastest)
python recon.py -c recon_config.yaml --show-sql           # print the SQL it will run, run nothing
python recon.py -c recon_config.yaml -v                   # log every SQL statement
```

Output in `output_dir`:
- `recon_<run>.xlsx`: Summary, Mismatched partitions, Differences, All partitions
- `recon_<run>.html`: one-page report to email or open in a browser
- `recon_history.csv`: one row per table per run; point Power BI at it for trends
- `recon.log`: log of every run (check this first when a scheduled run fails)
- Database tables `RECON_SUMMARY`, `RECON_PARTITIONS`, `RECON_DIFFERENCES` (see below)

### Saving results to the database

Turned on in `recon_config.yaml` under `results_db`. Each run appends rows tagged with
`RUN_ID` and `RUN_TIME`, so the tables build up a history you can report on in Power BI.

- Uses its own connection (`recon_db`) with a user that has `INSERT` on the three tables,
  so the source/destination users stay read-only. Add `RECON_PWD` to `.env`.
- Create the tables once with `sql/recon_tables_mssql.sql` (or `sql/recon_tables_oracle.sql`).
  If they don't exist the tool creates them, but with generic column types.
- All three tables are written in one transaction: a run is saved completely or not at all.
- If saving fails, the Excel/HTML reports are still written, the error is in `recon.log`,
  and the exit code is 2 so the scheduler flags it.
- Column names: `TABLE_NAME`, `PARTITION_KEY`, `RECORD_KEY`, `COLUMN_NAME` (renamed from the
  Excel headers to avoid SQL reserved words).
- To switch it off, comment out the `results_db` block.

Exit code: `0` all matched, `1` mismatches found, `2` a table errored. Schedulers can alert on it.

**Before first use on real data**, run `--show-sql` and try the printed queries in SQL
Developer / SSMS on one table. The SQL for Oracle, SQL Server, PostgreSQL and MySQL is
generated from the same templates, but the full end-to-end test was done on SQLite (see Demo).

## Web app (run recons from the browser)

```
python app.py                                  # this PC only: http://localhost:8080
python app.py --host 0.0.0.0 --port 8080       # for the team: http://<server-name>:8080
```
or double-click `run_web.bat`.

| Page | What you can do |
|---|---|
| Dashboard | See every table pair with its latest status; tick tables, choose level, **Run selected**; **Test connections** |
| Live log | Follow a run as it happens; **Stop** it; jump to the results when it finishes |
| Run results | Per-table summary, mismatched partitions, differences with filters and search; download Excel / HTML |
| Run history | Every run (browser, command line or scheduler) and who started it; connection test history |
| Configuration | Read-only view of connections (no passwords) and table pairs |

**Logins** go in `.env`: `UI_USERS=ravindra:pass1,amit:pass2` (can run and stop) and
`UI_VIEWERS=manager:pass3` (view only). Without logins the app accepts connections only from
the PC it runs on; it refuses `--host 0.0.0.0` until `UI_USERS` is set.

**Behaviour**
- One run at a time. While a run or connection test is going, the Run buttons are disabled
  for everyone and the dashboard shows who started it.
- Each run is a separate `recon.py` process, so the browser can be closed mid-run; the log
  and results are there when you come back.
- Runs from the command line or scheduler also appear in the dashboard and history.
- Install `waitress` (in requirements) for a production-grade server; without it Flask's
  built-in server is used.
- Logins use HTTP Basic authentication. On a shared network, put the app behind HTTPS
  (IIS reverse proxy or your load balancer) so passwords are not sent in clear text.
- To keep it running after logoff, run `run_web.bat` as a Windows service (e.g. NSSM) or a
  Task Scheduler task "At startup".

## Config reference (per table)

| Field | Meaning |
|-------|---------|
| `source` / `target` | `connection`, `table`, optional `filter` (SQL in that side's dialect) |
| `filter` | One filter for both sides (only if the SQL is valid on both) |
| `keys` | Unique key column(s): `[POLICY_NO]`, `[POLICY_NO, RIDER]` or `{SRC_COL: TGT_COL}` |
| `key_type` | `text` or `number` (integer keys only). Must be right: it decides the bucket SQL |
| `partition` | `column`, `grain` (`day` / `month` / `value`), optional `target_column` |
| `columns` | `NAME: number/text/date/datetime` or `NAME: {type, target, scale, tolerance, ignore_case, signature}` |
| `enabled` | `false` to skip without deleting |

## Performance tips

- **Partition by an indexed date column** (load date, business date). Every drill-down query
  filters on it, so the index keeps them fast.
- **Use filters for daily runs** (last 30-90 days) and run the full history weekly.
- Run the tool on a server close to the databases, not over VPN from a laptop.
- `parallel_tables` controls how many table pairs run together; raise it if the databases
  have spare capacity, lower it during business hours.
- Compare the columns that matter (amounts, status, dates, codes) rather than every column.

## Known limits

- A text change that keeps the same length **and** the same first, second and last
  character (e.g. `AB1C` -> `AB2C`) is invisible to the level-2 fingerprint. The random
  spot check catches such issues over successive runs; raise `spot_check_buckets` for
  critical tables.
- Text signatures use character codes. Hindi/Marathi or other non-English text can produce
  different codes on Oracle vs SQL Server, which shows up as "aggregates differ but no
  row-level difference". Set `signature: false` on such columns.
- Composite keys are bucketed on their first column.
- `number` keys must be whole numbers.

## Demo (no database needed)

```
python demo/make_demo.py            # builds demo/dwh.db and demo/mis.db (1,000,000 policies)
python recon.py -c demo/recon_demo.yaml
```

The target copy has 26 planted errors (missing, extra, changed premium incl. a 1-paisa
change, changed status, shifted dates, nulled agent codes, a duplicate key, a changed agent
name in a table whose column is renamed) plus cosmetic noise that must be ignored (trailing
spaces, lowercase codes, '' vs NULL, floating-point dust). The tool finds all 26 with zero
false positives. `demo/planted_errors.txt` lists them for checking.
