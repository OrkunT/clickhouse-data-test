#!/usr/bin/env bash
set -euo pipefail

# Benchmarks JSON-extraction of SG columns over a two-week window
DB="drill_tests"
RUNS=10
NUM_QUERIES=16
ITER=$((RUNS * NUM_QUERIES))
QUERIES_FILE="json_sg_queries.sql"
RESULTS_FILE="json_sg_results.txt"

# Generate exactly 16 one-line JSON-based SQL statements
cat > "$QUERIES_FILE" <<'EOF'
SELECT SUM(c) AS total_c, SUM(s) AS total_s, SUM(dur) AS total_dur, COUNT(DISTINCT uid) AS unique_uids FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND up_extra.d.:String='Android' SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String='Android' SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_extra.k0001.:String='Android' OR sg_extra.k0001.:String='iOS') SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_extra.k0001.:String='Android' OR sg_extra.k0002.:String='US') SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_extra.k0001.:String='iOS' AND sg_extra.k0002.:String='US') SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND ((sg_extra.k0001.:String='iOS' OR sg_extra.k0002.:String='US') AND sg_extra.k0003.:String='1.2') SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0004.:String LIKE '%dro%' SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0004.:String NOT LIKE '%dro%' SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String LIKE 'Andr%' SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String LIKE '%droid' SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String IS NOT NULL SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String IS NULL SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String!='Android' SETTINGS allow_experimental_json_type = 1;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_extra.k0001.:String!='Android' AND sg_extra.k0001.:String!='iOS') SETTINGS allow_experimental_json_type = 1;
SELECT sg_extra.k0002.:String AS segment_cc, sg_extra.k0005.:String AS hourly, sg_extra.k0006.:String AS daily, sg_extra.k0007.:String AS weekly, sg_extra.k0008.:String AS monthly, SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String='Android' GROUP BY segment_cc,hourly,daily,weekly,monthly WITH ROLLUP SETTINGS allow_experimental_json_type = 1;
SELECT sg_extra.k0002.:String AS segment_cc, sg_extra.k0003.:String AS browser, sg_extra.k0004.:String AS platform_version, sg_extra.k0005.:String AS hourly, sg_extra.k0006.:String AS daily, sg_extra.k0007.:String AS weekly, sg_extra.k0008.:String AS monthly, SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String='Android' GROUP BY segment_cc,browser,platform_version,hourly,daily,weekly,monthly WITH ROLLUP SETTINGS allow_experimental_json_type = 1;
EOF

# Run RUNS×16 and capture stderr (percentiles)
clickhouse-benchmark \
  --database="$DB" \
  -c 1 \
  -i $ITER \
  < "$QUERIES_FILE" \
  2> "$RESULTS_FILE"

echo "? JSON-SG results (2-week) in $RESULTS_FILE"
