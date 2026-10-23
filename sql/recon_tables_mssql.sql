-- Recon results tables for SQL Server. Run once (or ask your DBA to).
-- Change [dbo] if you set `schema:` under results_db in recon_config.yaml.
-- If you skip this script the tool creates the tables itself, but with generic column types.

CREATE TABLE dbo.RECON_SUMMARY (
    RUN_ID                 VARCHAR(20)     NOT NULL,
    RUN_TIME               DATETIME2(0)    NOT NULL,
    TABLE_NAME             VARCHAR(128)    NOT NULL,
    STATUS                 VARCHAR(10)     NOT NULL,   -- MATCHED / MISMATCH / ERROR
    SOURCE_TABLE           VARCHAR(300),
    TARGET_TABLE           VARCHAR(300),
    SOURCE_ROWS            BIGINT,
    TARGET_ROWS            BIGINT,
    ROW_DIFF               BIGINT,
    PARTITIONS             INT,
    PARTITIONS_MISMATCHED  INT,
    MISSING_IN_TARGET      INT,
    EXTRA_IN_TARGET        INT,
    VALUE_MISMATCHES       INT,
    MOVED_PARTITION        INT,
    DUPLICATE_KEYS         INT,
    ROWS_FETCHED           BIGINT,
    SPOT_CHECKED_BUCKETS   INT,
    DURATION_SECONDS       DECIMAL(10, 2),
    ERROR_MESSAGE          NVARCHAR(4000)
);
CREATE INDEX IX_RECON_SUMMARY ON dbo.RECON_SUMMARY (RUN_TIME, TABLE_NAME);

CREATE TABLE dbo.RECON_PARTITIONS (
    RUN_ID          VARCHAR(20)     NOT NULL,
    RUN_TIME        DATETIME2(0)    NOT NULL,
    TABLE_NAME      VARCHAR(128)    NOT NULL,
    PARTITION_KEY   NVARCHAR(200),
    SOURCE_ROWS     BIGINT,
    TARGET_ROWS     BIGINT,
    ROW_DIFF        BIGINT,
    STATUS          VARCHAR(10),
    FAILED_CHECKS   NVARCHAR(4000),
    NOTE            NVARCHAR(4000)
);
CREATE INDEX IX_RECON_PARTITIONS ON dbo.RECON_PARTITIONS (RUN_ID, TABLE_NAME);

CREATE TABLE dbo.RECON_DIFFERENCES (
    RUN_ID          VARCHAR(20)     NOT NULL,
    RUN_TIME        DATETIME2(0)    NOT NULL,
    TABLE_NAME      VARCHAR(128)    NOT NULL,
    PARTITION_KEY   NVARCHAR(200),
    RECORD_KEY      NVARCHAR(500),
    ISSUE           VARCHAR(40),    -- MISSING_IN_TARGET / EXTRA_IN_TARGET / VALUE_MISMATCH / ...
    COLUMN_NAME     VARCHAR(128),
    SOURCE_VALUE    NVARCHAR(4000),
    TARGET_VALUE    NVARCHAR(4000)
);
CREATE INDEX IX_RECON_DIFFERENCES ON dbo.RECON_DIFFERENCES (RUN_ID, TABLE_NAME);

-- Permissions for the tool's write user (replace recon_rw with your login's user):
-- GRANT SELECT, INSERT ON dbo.RECON_SUMMARY     TO recon_rw;
-- GRANT SELECT, INSERT ON dbo.RECON_PARTITIONS  TO recon_rw;
-- GRANT SELECT, INSERT ON dbo.RECON_DIFFERENCES TO recon_rw;

-- Housekeeping (e.g. monthly): keep 6 months of history
-- DELETE FROM dbo.RECON_DIFFERENCES WHERE RUN_TIME < DATEADD(MONTH, -6, GETDATE());
-- DELETE FROM dbo.RECON_PARTITIONS  WHERE RUN_TIME < DATEADD(MONTH, -6, GETDATE());
-- DELETE FROM dbo.RECON_SUMMARY     WHERE RUN_TIME < DATEADD(MONTH, -6, GETDATE());
