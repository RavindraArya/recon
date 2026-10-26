# DWH Reconciliation Tool: Architecture

This document explains how the tool is built, how each database connection is assembled
from the config, what has to be in place before a connection works, and how a run flows
from start to report. Diagrams are written in Mermaid, which renders in VS Code (with the
Markdown Preview Mermaid extension), GitHub, GitLab, Azure DevOps and Confluence.

---

## 1. Overview

The tool checks that tables loaded from the Oracle DWH into downstream databases (SQL
Server MIS) contain the same data. It never copies tables between databases. Each
database computes summaries of its own data with `SELECT ... GROUP BY` queries; the tool
compares the small summaries and pulls actual rows only for the slices that differ.

**Design principles**

| Principle | How it is applied |
|---|---|
| Push work to the databases | Counts, sums and fingerprints are computed in SQL; only aggregates travel over the network |
| Read-only on business data | Source and target connections only run `SELECT`; writes go to a separate results connection |
| Fail per table, not per run | One table erroring does not stop the others; it is reported as `ERROR` |
| Engine-neutral | All SQL comes from one template, rendered by a per-engine dialect class |
| No secrets in config | Passwords live in `.env` or environment variables, never in `recon_config.yaml` |

---

## 2. Component architecture

```mermaid
flowchart LR
    subgraph Server["Recon server / PC  (Python 3.10+)"]
        direction TB
        CFG["recon_config.yaml<br/>connections, tables, settings"]
        ENV[".env<br/>DWH_PWD, MIS_PWD, RECON_PWD"]
        WEB["app.py (web app)<br/>Flask + waitress, port 8080"]
        CLI["recon.py (CLI)<br/>--check / --show-sql / -t / -l"]
        subgraph Engine["Recon engine"]
            SPEC["Table specs<br/>(keys, columns, partition)"]
            DIA["Dialect layer<br/>Oracle / SQL Server / PG / MySQL / SQLite"]
            REC["Reconciler<br/>L1 counts, L2 aggregates, L3 drill-down"]
            DIFF["Row diff + normalise<br/>(pandas)"]
        end
        REP["Report writer"]
        OUT[("recon_output/<br/>xlsx, html, history.csv, recon.log")]
    end

    DWH[("Oracle DWH<br/>SOURCE<br/>read-only user")]
    MIS[("SQL Server MIS<br/>TARGET<br/>read-only user")]
    RES[("SQL Server MIS<br/>RECON_* tables<br/>insert-only user")]
    PBI["Power BI"]

    USERS["Browser users<br/>UI_USERS / UI_VIEWERS"] -->|"HTTP(S) + login"| WEB
    WEB -->|"starts as separate process"| CLI
    OUT -->|"recon_RUNID.json, logs"| WEB
    CFG --> CLI
    ENV --> CLI
    CLI --> SPEC --> DIA --> REC
    REC <-->|"oracledb (thin)<br/>port 1521<br/>SELECT only"| DWH
    REC <-->|"pyodbc + ODBC Driver<br/>port 1433<br/>SELECT only"| MIS
    REC --> DIFF --> REP
    REP --> OUT
    REP -->|"INSERT, one transaction"| RES
    RES --> PBI
    OUT -.->|history.csv| PBI
```

**Components**

| Component | Code (in `recon.py`) | Responsibility |
|---|---|---|
| CLI | `main()` | Parse arguments, load `.env`, load config, start log file, run tables in parallel |
| Env loader | `load_env_file()` | Read `KEY=VALUE` lines from `.env` into the environment |
| Connection | `Database` | Build the connection from config fields, run read-only queries, write results |
| Connection check | `check_connections()`, `connection_hint()` | `--check`: run `SELECT 1` on every connection, translate errors to fixes |
| Dialects | `OracleDialect`, `SqlServerDialect`, ... | Engine-specific SQL pieces (date formats, MOD, string functions, casts) |
| Table spec | `build_spec()`, `TableSpec`, `Column` | Validate a table block, map source/target names, catch `xxxx` placeholders |
| SQL builders | `profile_sql()`, `bucket_sql()`, `fetch_sql()` | Generate the level 1/2 profile, bucket fingerprint and row fetch queries |
| Reconciler | `Reconciler` | Run the levels for one table pair and collect differences |
| Normaliser | `_norm_frame()`, `_metric_equal()` | Make values from different engines comparable (trim, NULLs, rounding, dates) |
| Reports | `build_frames()`, `write_excel()`, `write_html()`, `db_frame()` | Excel, HTML, history CSV and database rows |

---

## 3. Folder layout

```
recon_tool/
├── recon.py                    the tool (single file)
├── app.py                      web app: run recons from the browser
├── templates/, static/         web app pages and styles
├── run_web.bat                 starts the web app
├── recon_config.yaml           connections + table pairs (no passwords)
├── .env                        passwords - create from .env.example, never share/commit
├── .env.example                template for .env
├── requirements.txt            Python packages
├── run_recon.bat               for Windows Task Scheduler
├── architecture.md             this document
├── README.md                   usage guide
├── sql/
│   ├── recon_tables_mssql.sql  CREATE TABLE for RECON_* (SQL Server)
│   └── recon_tables_oracle.sql CREATE TABLE for RECON_* (Oracle)
├── demo/                       self-contained demo on SQLite (no database needed)
└── recon_output/               created at run time: reports, history, recon.log
```

---

## 4. How a connection is built

### 4.1 From config to live connection

```mermaid
flowchart TD
    A["recon_config.yaml<br/>connections: dwh / mis / recon_db"] --> B{"Any value<br/>still xxxx?"}
    B -->|yes| B1["Stop: 'host is still xxxx'<br/>(names the field)"]
    B -->|no| C{"Field style<br/>or full url?"}
    C -->|"url: ..."| D["Use url as given<br/>replace {password}"]
    C -->|"host / server / user ..."| E["Build URL with<br/>sqlalchemy URL.create()"]
    P1[".env next to config<br/>or --env-file"] --> P2["Environment<br/>(real env vars win)"]
    P2 --> P3{"password_env<br/>found and not xxxx?"}
    P3 -->|no| P4["Stop: 'add DWH_PWD=... to .env'"]
    P3 -->|yes| E
    P3 -->|yes| D
    D --> F["create_engine()<br/>connection pool, pool_pre_ping"]
    E --> F
    F --> G["Driver"]
    G -->|Oracle| G1["oracledb thin mode<br/>TCP to host:1521"]
    G -->|SQL Server| G2["pyodbc -> ODBC Driver 17/18<br/>TCP to server:1433 or named instance"]
    G1 --> H[("Oracle DWH")]
    G2 --> I[("SQL Server")]
```

Notes:
- `create_engine()` does not open a connection; the first query does. That is why
  `--check` runs `SELECT 1` (Oracle: `SELECT 1 FROM DUAL`) to prove the link end to end.
- Passwords are URL-encoded by `URL.create()`, so `@`, `#`, `%`, `/` in passwords are safe.
- For Oracle the tool raises the fetch batch to 10,000 rows (`oracledb.defaults.arraysize`),
  which cuts network round trips when drill-down pulls rows.
- For SQL Server `fast_executemany` is switched on, which makes the `RECON_*` inserts fast.

### 4.2 Connection string produced for each engine

| Engine | Config fields | Connection string the tool builds |
|---|---|---|
| Oracle (service) | `host, port, service_name, user, password_env` | `oracle+oracledb://USER:PWD@HOST:1521/?service_name=SVC` |
| Oracle (SID) | `host, port, sid, user, password_env` | `oracle+oracledb://USER:PWD@HOST:1521/SID` |
| Oracle, thick mode | add `thick_mode: true, client_lib_dir` | Same string; driver loads Oracle Instant Client first |
| SQL Server, SQL login | `server, [port], database, driver, user, password_env` | `mssql+pyodbc://USER:PWD@SERVER[:PORT]/DB?driver=...&TrustServerCertificate=yes` |
| SQL Server, Windows login | `server, [port], database, driver, windows_auth: true` | `mssql+pyodbc://@SERVER/DB?driver=...&Trusted_Connection=yes&TrustServerCertificate=yes` |
| PostgreSQL | `host, port, database, user, password_env` | `postgresql+psycopg2://USER:PWD@HOST:5432/DB` |
| MySQL / MariaDB | `host, port, database, user, password_env` | `mysql+pymysql://USER:PWD@HOST:3306/DB` |
| SQLite (demo) | `path` | Opened read-only with Python's built-in `sqlite3` |

A full `url:` can still be given instead of the fields; `{password}` inside it is
replaced from `password_env`.

### 4.3 Password lookup order

1. Real environment variable (for example set in the scheduled task) - wins if present.
2. `.env` given with `--env-file path`.
3. `.env` in the same folder as the config file.
4. `.env` in the current folder.

`.env` format, one per line: `DWH_PWD=yourpassword` (quotes optional, `#` lines are comments).

---

## 5. What must be in place to connect

### 5.1 Checklist

| # | Item | Oracle DWH (source) | SQL Server MIS (target) | Results DB (`recon_db`) |
|---|---|---|---|---|
| 1 | Python package | `pip install oracledb` | `pip install pyodbc` | same as target |
| 2 | OS driver | **None** (thin mode, no Oracle Client). Only if `--check` asks for thick mode: Oracle Instant Client | **Microsoft ODBC Driver 17 or 18 for SQL Server** (admin install) | same as target |
| 3 | Network | PC/server can reach host on port 1521 | Reach server on 1433 (named instance: SQL Browser UDP 1434 or a fixed port) | same |
| 4 | DB user | Read-only user | Read-only user, or Windows login | User with `INSERT` on `RECON_*` |
| 5 | Grants | `CREATE SESSION` + `SELECT` on each source table | `SELECT` on each target table (or `db_datareader`) | `SELECT, INSERT` on `RECON_*` |
| 6 | Config fields | `host, port, service_name / sid, user` | `server, database, driver, windows_auth, user` | same as target |
| 7 | `.env` line | `DWH_PWD=...` | `MIS_PWD=...` (not needed for Windows login) | `RECON_PWD=...` |
| 8 | Objects | - | - | Run `sql/recon_tables_mssql.sql` once |

### 5.2 Where to find each value

| Value | Where to look |
|---|---|
| Oracle host, port, service name | SQL Developer: right-click connection > Properties; or `tnsnames.ora` (`HOST=`, `PORT=`, `SERVICE_NAME=`) |
| Oracle SID vs service name | If the connection uses `SID=` use `sid:`; otherwise `service_name:` |
| SQL Server name / instance | SSMS login screen, "Server name" box (`SERVER` or `SERVER\INSTANCE`) |
| ODBC driver name | `python -c "import pyodbc; print(pyodbc.drivers())"` and copy the name exactly |
| Windows vs SQL login | SSMS login screen, "Authentication" box |

### 5.3 Grants to request from the DBA

```sql
-- Oracle (DWH) - read-only user
CREATE USER RECON_RO IDENTIFIED BY "********";
GRANT CREATE SESSION TO RECON_RO;
GRANT SELECT ON DWH.POLICY_MASTER TO RECON_RO;     -- one per source table
GRANT SELECT ON DWH.AGENT_MASTER  TO RECON_RO;

-- SQL Server (MIS) - read-only user
CREATE LOGIN recon_ro WITH PASSWORD = '********';
USE MISDB;
CREATE USER recon_ro FOR LOGIN recon_ro;
ALTER ROLE db_datareader ADD MEMBER recon_ro;      -- or GRANT SELECT per table

-- SQL Server (MIS) - results writer
CREATE LOGIN recon_rw WITH PASSWORD = '********';
CREATE USER recon_rw FOR LOGIN recon_rw;
GRANT SELECT, INSERT ON dbo.RECON_SUMMARY     TO recon_rw;
GRANT SELECT, INSERT ON dbo.RECON_PARTITIONS  TO recon_rw;
GRANT SELECT, INSERT ON dbo.RECON_DIFFERENCES TO recon_rw;
```

### 5.4 Verifying connections

```
python recon.py -c recon_config.yaml --check
```

```mermaid
sequenceDiagram
    participant U as You
    participant T as recon.py --check
    participant O as Oracle DWH
    participant S as SQL Server
    U->>T: run --check
    T->>T: load .env, read config, reject xxxx placeholders
    T->>O: connect + SELECT 1 FROM DUAL
    O-->>T: OK / error
    T->>S: connect (mis) + SELECT 1
    S-->>T: OK / error
    T->>S: connect (recon_db) + SELECT 1
    S-->>T: OK / error
    T-->>U: OK or FAIL + plain-language fix per connection
```

| `--check` says | Meaning | Fix |
|---|---|---|
| `'host' ... is still 'xxxx'` | Placeholder not filled | Fill that field in `recon_config.yaml` |
| `password ... still the placeholder` / `password not found` | `.env` missing or not filled | Put the real password in `.env` |
| `Server name '...' not found` (getaddrinfo, 11001) | Host name wrong or not reachable by DNS | Correct host; check office network / VPN |
| `ODBC driver ... not installed` (IM002) | `driver:` name does not match an installed driver | Copy a name from `pyodbc.drivers()`, or install ODBC Driver 18 |
| `user name or password was rejected` (ORA-01017, 18456) | Credentials wrong | Check `user:` and `.env` |
| `service_name ... is wrong` (ORA-12514) | Wrong service name | Check `tnsnames.ora` / SQL Developer |
| `no answer on that port` (timeout, 10060) | Firewall or wrong port | Check port; ask network team to open it |
| `database name ... is wrong` (4060) | Database name or access | Check `database:` and user mapping |
| `needs 'thick mode'` (DPY-3015, DPY-3001) | Oracle DB uses an old password verifier or network encryption that thin mode does not support | Install Oracle Instant Client; set `thick_mode: true` and `client_lib_dir:` on `dwh` |

---

## 6. Run flow

```mermaid
sequenceDiagram
    autonumber
    participant CLI as recon.py
    participant R as Reconciler (per table, in parallel)
    participant S as Source (Oracle)
    participant T as Target (SQL Server)
    participant OUT as Reports / RECON_* tables

    CLI->>CLI: load .env, config, open recon.log
    CLI->>R: start up to parallel_tables tables at once
    par same query shape on both sides
        R->>S: Level 1+2 profile: GROUP BY partition (counts, sums, lengths, signatures, dates)
        R->>T: Level 1+2 profile: GROUP BY partition
    end
    R->>R: compare per partition -> MATCHED / MISMATCH
    loop each mismatched partition (Level 3)
        par
            R->>S: bucket fingerprints (GROUP BY MOD(key, N)) for this partition
            R->>T: bucket fingerprints
        end
        R->>R: find differing buckets
        par
            R->>S: fetch rows of differing buckets only
            R->>T: fetch rows of differing buckets only
        end
        R->>R: normalise + row diff -> MISSING / EXTRA / VALUE / DUPLICATE
    end
    R->>S: spot check: fetch 1-2 random buckets from matched partitions
    R->>T: same buckets
    R->>R: merge MISSING+EXTRA of same key -> MOVED_PARTITION
    R-->>CLI: TableResult
    CLI->>OUT: Excel, HTML, history.csv, RECON_* (one transaction)
    CLI-->>CLI: exit 0 / 1 / 2
```

### 6.1 Levels

| Level | Query per side | Detects | Cost |
|---|---|---|---|
| 1 | `COUNT(*) GROUP BY partition` | Missing / extra rows | One scan (index-only if partition column indexed) |
| 2 | Level 1 + per-column aggregates in the same scan | Changed amounts, statuses, dates, NULLs, duplicates | One scan |
| 3 | Bucket fingerprints + row fetch, only for failed partitions | Exact key, column, source and target value | Proportional to the number of bad slices |

### 6.2 Level 2 fingerprint per column type

| Type | Metrics computed in the database |
|---|---|
| `number` | `SUM(ROUND(col, scale))`, `COUNT(col)` |
| `text` | `SUM(LENGTH(TRIM(col)))`, `COUNT(non-blank)`, `SUM(signature)` where signature = codes of 1st, 2nd and last character |
| `date` / `datetime` | `MIN`, `MAX`, `COUNT`, `SUM(days or seconds since 1970)` |
| key (single column) | `COUNT(DISTINCT key)` -> duplicates |

### 6.3 Drill-down (Level 3)

```mermaid
flowchart TD
    A["Partition failed Level 2<br/>e.g. 2026-03: 4,66,90 rows"] --> B["N = rows / bucket_size<br/>e.g. 24 buckets of ~20,000"]
    B --> C["Both sides: GROUP BY bucket = MOD(hash(key), N)<br/>same fingerprints per bucket"]
    C --> D{"Bucket fingerprints<br/>equal?"}
    D -->|"23 equal"| E["Skip - never fetched"]
    D -->|"1 differs"| F["Fetch ~20,000 rows from each side<br/>(only that bucket)"]
    F --> G["Normalise: trim, '' = NULL,<br/>round numbers, format dates, case"]
    G --> H["Outer join on key"]
    H --> I["MISSING_IN_TARGET<br/>EXTRA_IN_TARGET<br/>VALUE_MISMATCH (column, both values)<br/>DUPLICATE_KEY"]
```

Bucketing uses only portable SQL, so source and target put the same key in the same
bucket: `MOD(key, N)` for number keys; for text keys, a weighted sum of the character
codes of the last 4 characters (padded with `####`), then `MOD N`.

### 6.4 Normalisation (why harmless differences are ignored)

| Difference | Handled by |
|---|---|
| Leading/trailing spaces, CHAR padding | `TRIM` in SQL, `strip()` in Python |
| `''` vs `NULL` (Oracle stores `''` as NULL) | `NULLIF(TRIM(col), '')` |
| Float noise (`100.000000001`) | `ROUND(col, scale)` before summing / comparing |
| Time part on `date` columns | `TRUNC` / `CAST AS DATE`; compare `YYYY-MM-DD` |
| Upper/lower case | `ignore_case: true` on the column |
| Rounding tolerance | `tolerance:` per number column |

---

## 7. Data model: results tables

```mermaid
erDiagram
    RECON_SUMMARY ||--o{ RECON_PARTITIONS : "RUN_ID + TABLE_NAME"
    RECON_SUMMARY ||--o{ RECON_DIFFERENCES : "RUN_ID + TABLE_NAME"
    RECON_SUMMARY {
        varchar RUN_ID
        datetime RUN_TIME
        varchar TABLE_NAME
        varchar STATUS "MATCHED / MISMATCH / ERROR"
        bigint SOURCE_ROWS
        bigint TARGET_ROWS
        bigint ROW_DIFF
        int PARTITIONS_MISMATCHED
        int MISSING_IN_TARGET
        int EXTRA_IN_TARGET
        int VALUE_MISMATCHES
        int MOVED_PARTITION
        int DUPLICATE_KEYS
        decimal DURATION_SECONDS
        nvarchar ERROR_MESSAGE
    }
    RECON_PARTITIONS {
        varchar RUN_ID
        datetime RUN_TIME
        varchar TABLE_NAME
        nvarchar PARTITION_KEY
        bigint SOURCE_ROWS
        bigint TARGET_ROWS
        varchar STATUS
        nvarchar FAILED_CHECKS
        nvarchar NOTE
    }
    RECON_DIFFERENCES {
        varchar RUN_ID
        datetime RUN_TIME
        varchar TABLE_NAME
        nvarchar PARTITION_KEY
        nvarchar RECORD_KEY
        varchar ISSUE
        varchar COLUMN_NAME
        nvarchar SOURCE_VALUE
        nvarchar TARGET_VALUE
    }
```

- All three tables are written in **one transaction** per run.
- `RECON_DIFFERENCES` holds up to `max_diff_rows` rows per table; the full counts are in
  `RECON_SUMMARY`.
- The same data is in the Excel report (sheets Summary, Mismatched partitions,
  Differences, All partitions) and in `recon_history.csv`.

---

## 8. Security model

```mermaid
flowchart LR
    subgraph Secrets
        E[".env<br/>(server only,<br/>folder ACL)"]
    end
    subgraph Users
        U1["RECON_RO (Oracle)<br/>SELECT on source tables"]
        U2["recon_ro (SQL Server)<br/>SELECT on target tables"]
        U3["recon_rw (SQL Server)<br/>INSERT on RECON_* only"]
    end
    E --> U1 & U2 & U3
    U1 -->|read| DWH[("DWH")]
    U2 -->|read| MIS[("MIS")]
    U3 -->|insert| RT[("RECON_* tables")]
```

- Business tables are never modified: the tool issues only `SELECT` against `dwh` and `mis`.
- `.env` is listed in `.gitignore`. Keep it on the server, restrict the folder to the
  service account, never email it.
- Reports contain key values and data values from mismatches. Treat `recon_output/` and
  `RECON_DIFFERENCES` with the same access controls as the source data.
- `--show-sql` lets the DBA review every query before the first run.
- Table, column and key names in the config are checked against a strict name pattern, and
  filters may not contain `;`, `--` or `/*`, so a config mistake cannot turn into extra SQL
  statements.
- Values from the data are always written to Excel as text, never as formulas (protects
  against formula injection when reports are opened).

---

## 9. Deployment and scheduling

```mermaid
flowchart LR
    DWHLOAD["DWH nightly load<br/>finishes"] --> TS["Windows Task Scheduler<br/>run_recon.bat<br/>(service account)"]
    TS --> RUN["recon.py<br/>daily: last 90 days (filters)<br/>weekly: full history"]
    RUN --> X{"Exit code"}
    X -->|0| OK["All matched"]
    X -->|1| MM["Mismatches -> check report"]
    X -->|2| ER["Error -> check recon.log"]
    RUN --> PB["RECON_* tables -> Power BI dashboard"]
```

| Setting | Recommendation |
|---|---|
| Where to run | A server or VM in the same network as the databases, not a laptop on VPN |
| When | After the DWH load completes, outside business hours |
| Daily scope | Per-side `filter:` for recent data (e.g. last 90 days) |
| Full scope | Weekly or monthly run without filters, or a second config file |
| Partition column | An indexed date column (load date / business date) |
| Housekeeping | Delete old reports and `RECON_*` rows (script in `sql/`) |

**Exit codes:** `0` all matched, `1` mismatches found, `2` a table failed or results
could not be saved.

---

## 10. Performance characteristics

| Factor | Effect |
|---|---|
| Data moved on a clean run | One summary row per partition per side |
| Data moved on a mismatch | About `bucket_size` rows per differing bucket per side |
| Parallelism | `parallel_tables` table pairs at once; source and target queried at the same time |
| Safety caps | `max_fetch_rows` (rows pulled per table), `max_diff_rows` (rows recorded), `max_buckets` |
| Database load | One full scan per table per side at Level 2; drill-down queries hit only one partition each (index on partition column) |

---

## 11. Extending the tool

**Add a table pair:** copy a block under `tables:` in `recon_config.yaml`, fill it in, run
`--show-sql -t <name>` to review the SQL, then `-t <name>` to run just that table.

**Add a database engine:** create a `Dialect` subclass overriding the functions that differ
(`mod`, `trim`, `length`, `substr`, `right`, `concat`, `to_day`, `fmt_day`, `fmt_month`,
`date_lit`, `num`, `epoch_days`, `epoch_secs`), register it in `DIALECTS`, and add a branch
in `Database._url()` for its connection fields.

**Planned / optional additions**
- Email the HTML report after each run (SMTP / Outlook).
- Query timeout so a slow query cannot hang a scheduled run.

---

## 12. Web app (`app.py`)

### 12.1 How a browser-triggered run works

```mermaid
sequenceDiagram
    autonumber
    participant B as Browser (user)
    participant W as app.py (Flask)
    participant P as recon.py process
    participant D as Databases
    participant F as recon_output/

    B->>W: POST /run (tables, level) with login
    W->>W: check login + same-origin, check no other run is active
    W->>F: record job in ui_jobs.json (who, when, tables)
    W->>P: start recon.py -c config --run-id ID -l N -t ...
    W-->>B: redirect to /jobs/ID (live log page)
    P->>D: same queries as a command-line run
    P->>F: log lines -> ui_logs/ID.log
    loop every 1.5 s
        B->>W: GET /api/jobs/ID
        W->>F: read status + log tail
        W-->>B: status, log, duration
    end
    P->>F: recon_ID.xlsx / .html / .json, RECON_* tables
    P-->>W: exit code 0 / 1 / 2
    W->>F: job status MATCHED / MISMATCH / ERROR
    B->>W: GET /runs/ID
    W->>F: read recon_ID.json
    W-->>B: results page (filters, search, downloads)
```

### 12.2 Pages and endpoints

| URL | Method | Purpose | Who |
|---|---|---|---|
| `/` | GET | Dashboard: table pairs, latest status, recent activity | users, viewers |
| `/run` | POST | Start a recon for selected tables and level | users |
| `/check` | POST | Start a connection test (`recon.py --check`) | users |
| `/jobs/<id>` | GET | Live log page | users, viewers |
| `/api/jobs/<id>` | GET | Status + log tail (JSON, polled by the page) | users, viewers |
| `/jobs/<id>/stop` | POST | Stop a running job | users |
| `/runs` | GET | Run history and connection test history | users, viewers |
| `/runs/<run_id>` | GET | Results of one run | users, viewers |
| `/download/<file>` | GET | Excel / HTML report of a run, or `recon_history.csv` | users, viewers |
| `/config` | GET | Read-only view of connections (passwords hidden) and tables | users, viewers |

### 12.3 Files the web app keeps (in `output_dir`)

| File | Content |
|---|---|
| `ui_jobs.json` | Every browser-started run/check: id, user, tables, level, start/end, status (last 500) |
| `ui_logs/<id>.log` | Full log of that run/check |
| `recon_<id>.json` | Results written by `recon.py` (also for command-line and scheduled runs) |

### 12.4 Web security

| Control | Implementation |
|---|---|
| Login | HTTP Basic against `UI_USERS` / `UI_VIEWERS` in `.env`; constant-time password compare |
| Roles | `UI_USERS` can run/stop; `UI_VIEWERS` can only view |
| No open access by accident | Without logins the app only serves `127.0.0.1`; `--host 0.0.0.0` is refused |
| Cross-site requests | Every POST must come from the app's own pages (Origin / Referer check) |
| One run at a time | Job lock; a second run is refused with who/when of the active one |
| File access | Downloads limited to `recon_<id>.xlsx/.html` and `recon_history.csv` by exact pattern |
| Secrets | Database passwords stay in `.env`, read only by `recon.py`; never shown in pages |
| Transport | Put behind HTTPS (IIS reverse proxy / load balancer) when used over the network |
| DNS rebinding | Host header must be localhost (no-login mode) or a name in `UI_ALLOWED_HOSTS` |
| Password guessing | 10 wrong passwords from one IP blocks it for 15 minutes |
| Browser headers | CSP, `X-Frame-Options: DENY` (no clickjacking), `nosniff`, `Cache-Control: no-store` |

### 12.5 Hosting

```mermaid
flowchart LR
    U1["Team browsers"] -->|"https://recon.company.local"| RP["IIS / reverse proxy<br/>(HTTPS)"]
    RP -->|"http://127.0.0.1:8080"| APP["app.py on recon server<br/>(Windows service via NSSM<br/>or Task Scheduler 'At startup')"]
    APP --> RECON["recon.py processes"]
    RECON --> DB1[("Oracle DWH")]
    RECON --> DB2[("SQL Server MIS")]
    TS["Task Scheduler<br/>nightly run_recon.bat"] --> RECON
```

---

## 13. Known limits

- A text change that keeps the same length and the same 1st, 2nd and last character is not
  visible to the Level 2 fingerprint; the random spot check catches it over time.
- Text signatures use character codes, which can differ between Oracle and SQL Server for
  non-English text; set `signature: false` on such columns.
- Composite keys are bucketed on their first column.
- `key_type: number` requires whole-number keys.
- Tested end to end on SQLite; Oracle and SQL Server SQL is generated from the same
  templates and should be reviewed with `--show-sql` on first use.