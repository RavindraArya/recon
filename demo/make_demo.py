"""
Builds two SQLite databases to try the recon tool without touching real systems:
  demo/dwh.db  - "source" (DWH)
  demo/mis.db  - "destination", a copy with planted errors + harmless cosmetic noise

python demo/make_demo.py [rows]     (default 1,000,000 policies)
"""
import datetime as dt
import os
import random
import shutil
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC, TGT = os.path.join(HERE, "dwh.db"), os.path.join(HERE, "mis.db")
N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
rnd = random.Random(42)

for p in (SRC, TGT):
    if os.path.exists(p):
        os.remove(p)

con = sqlite3.connect(SRC)
con.executescript("""
CREATE TABLE POLICY_MASTER (POLICY_NO TEXT PRIMARY KEY, AGENT_CODE TEXT, PRODUCT TEXT,
  PREMIUM REAL, SUM_ASSURED REAL, STATUS TEXT, ISSUE_DATE TEXT, BRANCH TEXT, REMARKS TEXT);
CREATE TABLE AGENT_MASTER (AGENT_ID INTEGER PRIMARY KEY, AGENT_NAME TEXT, BRANCH TEXT,
  JOIN_DATE TEXT, ACTIVE TEXT);
""")
start = dt.date(2025, 1, 1)
products = ["TERM", "ULIP", "ENDOW", "PENSION", "HEALTH"]
statuses = ["INFORCE", "LAPSED", "PAIDUP", "SURRENDERED"]
branches = [f"BR{i:03d}" for i in range(1, 121)]


def policies():
    for i in range(N):
        yield (f"P{100000000 + i}", f"AG{rnd.randint(1, 50000):06d}", rnd.choice(products),
               round(rnd.uniform(5000, 250000), 2), round(rnd.uniform(1e5, 5e7), 2),
               rnd.choice(statuses), (start + dt.timedelta(days=rnd.randint(0, 660))).isoformat(),
               rnd.choice(branches), rnd.choice([None, "", "OK", "Check KYC"]))


con.executemany("INSERT INTO POLICY_MASTER VALUES (?,?,?,?,?,?,?,?,?)", policies())
con.executemany("INSERT INTO AGENT_MASTER VALUES (?,?,?,?,?)",
                ((i, f"Agent {i}", rnd.choice(branches),
                  (dt.date(2015, 1, 1) + dt.timedelta(days=rnd.randint(0, 3800))).isoformat(),
                  rnd.choice(["Y", "N"])) for i in range(1, 50001)))
con.execute("CREATE INDEX IX_PM_ISSUE ON POLICY_MASTER(ISSUE_DATE)")
con.commit()
con.close()
shutil.copy(SRC, TGT)

t = sqlite3.connect(TGT)
pick = lambda: f"P{100000000 + rnd.randrange(N)}"  # noqa: E731
planted = []

# ---- real errors the tool must find --------------------------------------
for _ in range(7):
    k = pick(); t.execute("DELETE FROM POLICY_MASTER WHERE POLICY_NO=?", (k,))
    planted.append(("MISSING_IN_TARGET", k))
for i in range(3):
    k = f"P9{i:08d}"
    t.execute("INSERT INTO POLICY_MASTER VALUES (?,?,?,?,?,?,?,?,?)",
              (k, "AG000001", "TERM", 10000, 1e6, "INFORCE", "2026-03-15", "BR001", None))
    planted.append(("EXTRA_IN_TARGET", k))
for _ in range(5):
    k = pick(); t.execute("UPDATE POLICY_MASTER SET PREMIUM = PREMIUM + 100 WHERE POLICY_NO=?", (k,))
    planted.append(("VALUE_MISMATCH PREMIUM", k))
for _ in range(1):
    k = pick(); t.execute("UPDATE POLICY_MASTER SET PREMIUM = PREMIUM + 0.01 WHERE POLICY_NO=?", (k,))
    planted.append(("VALUE_MISMATCH PREMIUM (1 paisa)", k))
for _ in range(4):
    k = pick(); t.execute("UPDATE POLICY_MASTER SET STATUS='FORECLOSED' WHERE POLICY_NO=?", (k,))
    planted.append(("VALUE_MISMATCH STATUS", k))
for _ in range(2):
    k = pick(); t.execute("UPDATE POLICY_MASTER SET ISSUE_DATE = DATE(ISSUE_DATE, '+1 day') WHERE POLICY_NO=?", (k,))
    planted.append(("ISSUE_DATE shifted 1 day", k))
for _ in range(2):
    k = pick(); t.execute("UPDATE POLICY_MASTER SET AGENT_CODE = NULL WHERE POLICY_NO=?", (k,))
    planted.append(("VALUE_MISMATCH AGENT_CODE (nulled)", k))

# duplicate key in target (PK must be dropped in a copy table)
t.executescript("""
CREATE TABLE PM2 AS SELECT * FROM POLICY_MASTER;
DROP TABLE POLICY_MASTER; ALTER TABLE PM2 RENAME TO POLICY_MASTER;
CREATE INDEX IX_PM_ISSUE ON POLICY_MASTER(ISSUE_DATE);
""")
k = pick()
t.execute("INSERT INTO POLICY_MASTER SELECT * FROM POLICY_MASTER WHERE POLICY_NO=?", (k,))
planted.append(("DUPLICATE_KEY_IN_TARGET", k))

# Agent table: renamed column in target + one real change
t.execute("ALTER TABLE AGENT_MASTER RENAME COLUMN AGENT_NAME TO AGNT_NAME")
t.execute("UPDATE AGENT_MASTER SET AGNT_NAME='Agent X' WHERE AGENT_ID=777")
planted.append(("VALUE_MISMATCH AGENT_NAME", "777"))

# ---- cosmetic noise the tool must IGNORE ---------------------------------
t.execute("UPDATE POLICY_MASTER SET STATUS = STATUS || '   ' WHERE rowid % 97 = 0")       # trailing spaces
t.execute("UPDATE POLICY_MASTER SET AGENT_CODE = LOWER(AGENT_CODE) WHERE rowid % 89 = 0")  # case (ignore_case)
t.execute("UPDATE POLICY_MASTER SET REMARKS = NULL WHERE REMARKS = ''")                    # '' vs NULL
t.execute("UPDATE POLICY_MASTER SET SUM_ASSURED = SUM_ASSURED + 0.0000001 WHERE rowid % 50 = 0")  # float noise
t.commit()
t.close()

with open(os.path.join(HERE, "planted_errors.txt"), "w") as f:
    for issue, key in planted:
        f.write(f"{issue}\t{key}\n")
print(f"Built {SRC} and {TGT} with {N:,} policies; {len(planted)} planted errors "
      f"listed in demo/planted_errors.txt")
