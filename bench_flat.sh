#!/usr/bin/env bash
set -euo pipefail

# Benchmarks flattened-SG columns over a two-week window
DB="drill_tests"
RUNS=10
NUM_QUERIES=16
ITER=$((RUNS * NUM_QUERIES))
QUERIES_FILE="flat_sg_queries.sql"
RESULTS_FILE="flat_sg_results.txt"

# Generate exactly 16 one-line SQL statements (no blank lines)
cat > "$QUERIES_FILE" <<'EOF'
SELECT SUM(c) AS total_c, SUM(s) AS total_s, SUM(dur) AS total_dur, COUNT(DISTINCT uid) AS unique_uids FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND up_d='Android';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001='Android';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_k0001='Android' OR sg_k0001='iOS');
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_k0001='Android' OR sg_k0002='US');
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_k0001='iOS' AND sg_k0002='US');
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND ((sg_k0001='iOS' OR sg_k0002='US') AND sg_k0003='1.2');
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0004 LIKE '%dro%';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0004 NOT LIKE '%dro%';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001 LIKE 'Andr%';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001 LIKE '%droid';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001 IS NOT NULL;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001 IS NULL;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001!='Android';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_k0001!='Android' AND sg_k0001!='iOS');
SELECT sg_k0002 AS segment_cc, sg_k0005 AS hourly, sg_k0006 AS daily, sg_k0007 AS weekly, sg_k0008 AS monthly, SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001='Android' GROUP BY segment_cc,hourly,daily,weekly,monthly WITH ROLLUP;
SELECT sg_k0002 AS segment_cc, sg_k0003 AS browser, sg_k0004 AS platform_version, sg_k0005 AS hourly, sg_k0006 AS daily, sg_k0007 AS weekly, sg_k0008 AS monthly, SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001='Android' GROUP BY segment_cc,browser,platform_version,hourly,daily,weekly,monthly WITH ROLLUP;
EOF

# Run RUNS×16 and capture stderr
clickhouse-benchmark \
  --database="$DB" \
  -c 1 \
  -i $ITER \
  < "$QUERIES_FILE" 2> "$RESULTS_FILE"

echo "? Flattened-SG results (2-week) in $RESULTS_FILE"
