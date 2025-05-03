#!/usr/bin/env bash
set -euo pipefail

# Configuration
db="drill_tests"
src_table="drill_events_8K"
mix_table="drill_events_mix_8K"
clickhouse_client="clickhouse-client"
sql_file="create_${mix_table}.sql"

# Generate SQL
cat > "${sql_file}" <<EOF
-- Drop existing mix table and recreate with JSON capped at 128 paths
DROP TABLE IF EXISTS ${db}.${mix_table};

CREATE TABLE ${db}.${mix_table}
(
    -- Original header columns
    a      LowCardinality(String),
    e      LowCardinality(String),
    uid    String,
    did    String,
    lsid   String,
    _id    String,
    ts     UInt64,

    -- Flattened up.* fields
    up_fs     UInt32,
    up_ls     UInt32,
    up_sc     UInt8,
    up_d      String,
    up_cty    String,
    up_rgn    String,
    up_cc     String,
    up_p      String,
    up_pv     String,
    up_av     String,
    up_c      String,
    up_r      String,
    up_brw    String,
    up_brwv   String,
    up_la     String,
    up_src    String,
    up_src_ch String,
    up_lv     String,
    up_hour   UInt8,
    up_dow    UInt8,

    -- JSON columns with max_dynamic_paths = 128
    up_extra  JSON(max_dynamic_paths = 128),
    custom    JSON(max_dynamic_paths = 128),
    cmp       JSON(max_dynamic_paths = 128),

    -- Flattened sg_k0001…sg_k0128 columns
EOF
# Append sg_k columns
for i in $(seq 1 128); do
    printf "    sg_k%04d String,\n" "$i" >> "${sql_file}"
done
# Continue with sg_extra and metrics
cat >> "${sql_file}" <<EOF
    -- Remaining sg JSON with capped paths
    sg_extra  JSON(max_dynamic_paths = 128),

    -- Metrics columns
    c      UInt32,
    s      Float64,
    dur    UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(ts))
ORDER BY (a, e, ts)
SETTINGS index_granularity = 8192;

-- Bulk-insert data with JSON extraction and metrics
INSERT INTO ${db}.${mix_table}
SELECT
    a,
    e,
    uid,
    did,
    lsid,
    _id,
    ts,

    -- Flatten up.*
    up.fs.:UInt32    AS up_fs,
    up.ls.:UInt32    AS up_ls,
    up.sc.:UInt8     AS up_sc,
    up.d.:String     AS up_d,
    up.cty.:String   AS up_cty,
    up.rgn.:String   AS up_rgn,
    up.cc.:String    AS up_cc,
    up.p.:String     AS up_p,
    up.pv.:String    AS up_pv,
    up.av.:String    AS up_av,
    up.c.:String     AS up_c,
    up.r.:String     AS up_r,
    up.brw.:String   AS up_brw,
    up.brwv.:String  AS up_brwv,
    up.la.:String    AS up_la,
    up.src.:String   AS up_src,
    up.src_ch.:String AS up_src_ch,
    up.lv.:String    AS up_lv,
    up.hour.:UInt8   AS up_hour,
    up.dow.:UInt8    AS up_dow,

    -- Preserve JSON tails
    up               AS up_extra,
    custom,
    cmp,
EOF
# Append sg JSON extraction
for i in $(seq 1 128); do
    printf "    sg.k%04d.:String AS sg_k%04d,\n" "$i" "$i" >> "${sql_file}"
done
# Finalize INSERT
cat >> "${sql_file}" <<EOF
    sg               AS sg_extra,
    c,
    s,
    dur
FROM ${db}.${src_table}
SETTINGS allow_experimental_json_type = 1;
EOF

# Execute the SQL file
echo "Executing SQL to recreate ${db}.${mix_table} with capped JSON paths..."
${clickhouse_client} --database="${db}" --multiquery < "${sql_file}"
echo "Done: ${db}.${mix_table} recreated with JSON(max_dynamic_paths=128)."
