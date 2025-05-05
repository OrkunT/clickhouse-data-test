#!/usr/bin/env bash
set -euo pipefail

SOURCE_TABLE="drill_events_mix_8K"
TARGET_TABLE="drill_events_mix_8K_capped"
DB="drill_tests"

echo "Dropping existing table if it exists..."
clickhouse-client --database="$DB" --query="DROP TABLE IF EXISTS $TARGET_TABLE"

echo "Creating $DB.$TARGET_TABLE schema with sg_extra as JSON(max_dynamic_paths=128)..."

# Generate sg_k**** column definitions and sg_extra map expression
SG_COLUMNS=()
SG_DEFS=()
SG_MAP=()
for i in $(seq 1 128); do
  key=$(printf "k%04d" "$i")
  col="sg_${key}"
  SG_COLUMNS+=("$col")
  SG_DEFS+=("$col String")
  SG_MAP+=("'$key', $col")
done

SG_COLUMNS_SQL=$(IFS=,; echo "${SG_COLUMNS[*]}")
SG_DEFS_SQL=$(IFS=,; echo "${SG_DEFS[*]}")
SG_MAP_SQL=$(IFS=,; echo "${SG_MAP[*]}")

# Create table with explicit schema
clickhouse-client --database="$DB" --query="
CREATE TABLE $TARGET_TABLE
(
    a LowCardinality(String),
    e LowCardinality(String),
    uid String,
    did String,
    lsid String,
    _id String,
    ts UInt64,
    up_fs UInt32,
    up_ls UInt32,
    up_sc UInt8,
    up_d String,
    up_cty String,
    up_rgn String,
    up_cc String,
    up_p String,
    up_pv String,
    up_av String,
    up_c String,
    up_r String,
    up_brw String,
    up_brwv String,
    up_la String,
    up_src String,
    up_src_ch String,
    up_lv String,
    up_hour UInt8,
    up_dow UInt8,
    custom JSON(max_dynamic_paths=128),
    cmp JSON(max_dynamic_paths=128),
    ${SG_DEFS_SQL},
    c UInt32,
    s Float64,
    dur UInt32,
    up_extra JSON(max_dynamic_paths=128),
    sg_extra JSON(max_dynamic_paths=128)
)
ENGINE = MergeTree()
ORDER BY ts
"

echo "Inserting data into $TARGET_TABLE..."
clickhouse-client --database="$DB" --query="
INSERT INTO $TARGET_TABLE
SELECT
    a, e, uid, did, lsid, _id, ts,
    up_fs, up_ls, up_sc, up_d,
    up_cty, up_rgn, up_cc, up_p,
    up_pv, up_av, up_c, up_r,
    up_brw, up_brwv, up_la,
    up_src, up_src_ch, up_lv,
    up_hour, up_dow,
    custom, cmp,
    ${SG_COLUMNS_SQL},
    c, s, dur,
    up_extra,
    map(${SG_MAP_SQL}) AS sg_extra
FROM $SOURCE_TABLE
"
