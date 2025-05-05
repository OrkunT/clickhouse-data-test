#!/usr/bin/env bash 
set -euo pipefail

DB="drill_tests"
RUNS=20
QUERIES_FILE="json_sg_queries_capped.sql"
RESULTS_FILE="json_sg_per_query_avg_capped.txt"

cat > "$QUERIES_FILE" <<'EOF'
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND up_extra.d.:String='Android';
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String='Android';
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_extra.k0001.:String='Android' OR sg_extra.k0001.:String='iOS');
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_extra.k0001.:String='Android' OR sg_extra.k0002.:String='US');
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_extra.k0001.:String='iOS' AND sg_extra.k0002.:String='US');
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND ((sg_extra.k0001.:String='iOS' OR sg_extra.k0002.:String='US') AND sg_extra.k0003.:String='1.2');
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0004.:String LIKE '%dro%';
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0004.:String NOT LIKE '%dro%';
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String LIKE 'Andr%';
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String LIKE '%droid';
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String IS NOT NULL;
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String IS NULL;
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_extra.k0001.:String!='Android';
SELECT SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_extra.k0001.:String!='Android' AND sg_extra.k0001.:String!='iOS');
SELECT sg_extra.k0002.:String segment_cc, sg_extra.k0003.:String browser, sg_extra.k0004.:String platform_version, sg_extra.k0005.:String hourly, sg_extra.k0006.:String daily, sg_extra.k0007.:String weekly, sg_extra.k0008.:String monthly, SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND up_extra.d.:String='Android' AND sg_extra.k0001.:String='ipsum' GROUP BY segment_cc,browser,platform_version,hourly,daily,weekly,monthly WITH ROLLUP;
SELECT sg_extra.k0002.:String segment_cc, sg_extra.k0003.:String browser, sg_extra.k0004.:String platform_version, sg_extra.k0005.:String hourly, sg_extra.k0006.:String daily, sg_extra.k0007.:String weekly, sg_extra.k0008.:String monthly, SUM(c), SUM(s), SUM(dur), COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND up_extra.d.:String='Android' AND sg_extra.k0001.:String='ipsum' GROUP BY segment_cc,browser,platform_version,hourly,daily,weekly,monthly WITH ROLLUP;
EOF

printf "Q#\tAvg_Time_s\n" > "$RESULTS_FILE"
for i in $(seq 1 $(wc -l < "$QUERIES_FILE")); do
  sum=0
  for run in $(seq 1 $RUNS); do
    start=$(date +%s.%N)
    clickhouse-client --database="$DB" --query="$(sed -n "${i}p" "$QUERIES_FILE")" --format Null >/dev/null
    sum=$(awk "BEGIN {print $sum + $(date +%s.%N) - $start}")
  done
  avg=$(awk "BEGIN {printf \"%.3f\", $sum/$RUNS}")
  printf "%2d\t%s\n" "$i" "$avg" >> "$RESULTS_FILE"
done

echo "? JSON-SG per-query averages written to $RESULTS_FILE"
