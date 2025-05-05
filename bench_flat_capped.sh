#!/usr/bin/env bash
set -euo pipefail

DB="drill_tests"
RUNS=20
QUERIES_FILE="flat_sg_queries_capped.sql"
RESULTS_FILE="flat_sg_per_query_avg_capped.txt"

# 1) Write your 16 SQL statements
cat > "$QUERIES_FILE" <<'EOF'
SELECT SUM(c) AS total_c, SUM(s) AS total_s, SUM(dur) AS total_dur, COUNT(DISTINCT uid) AS unique_uids FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND up_d='Android';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001='Android';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_k0001='Android' OR sg_k0001='iOS');
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_k0001='Android' OR sg_k0002='US');
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_k0001='iOS' AND sg_k0002='US');
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND ((sg_k0001='iOS' OR sg_k0002='US') AND sg_k0003='1.2');
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0004 LIKE '%dro%';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0004 NOT LIKE '%dro%';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001 LIKE 'Andr%';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001 LIKE '%droid';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001 IS NOT NULL;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001 IS NULL;
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND sg_k0001!='Android';
SELECT SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND (sg_k0001!='Android' AND sg_k0001!='iOS');
SELECT sg_k0002 AS segment_cc, sg_k0003 AS browser, sg_k0004 AS platform_version, sg_k0005 AS hourly, sg_k0006 AS daily, sg_k0007 AS weekly, sg_k0008 AS monthly, SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND up_d='Android' AND sg_k0001='ipsum' GROUP BY segment_cc,browser,platform_version,hourly,daily,weekly,monthly WITH ROLLUP;
SELECT sg_k0002 AS segment_cc, sg_k0003 AS browser, sg_k0004 AS platform_version, sg_k0005 AS hourly, sg_k0006 AS daily, sg_k0007 AS weekly, sg_k0008 AS monthly, SUM(c),SUM(s),SUM(dur),COUNT(DISTINCT uid) FROM drill_events_mix_8K_capped WHERE ts>=1745088420408 AND ts<=1746298020408 AND up_d='Android' AND sg_k0001='ipsum' GROUP BY segment_cc,browser,platform_version,hourly,daily,weekly,monthly WITH ROLLUP;
EOF

# 2) Read queries
mapfile -t QUERIES < <(sed 's/;[[:space:]]*$//' "$QUERIES_FILE")

# 3) Prepare output
printf "Q#\tAvg_Time_s\n" > "$RESULTS_FILE"

# 4) Loop, measure each 10×, compute arithmetic mean
for i in "${!QUERIES[@]}"; do
  idx=$((i+1))
  sql="${QUERIES[i]}"
  sum=0
  for run in $(seq 1 $RUNS); do
    start=$(date +%s.%N)
    clickhouse-client --database="$DB" --query="$sql" --format Null >/dev/null
    end=$(date +%s.%N)
    t=$(awk "BEGIN {print $end - $start}")
    sum=$(awk "BEGIN {print $sum + $t}")
  done
  avg=$(awk "BEGIN {printf \"%.3f\", $sum / $RUNS}")
  printf "%2d\t%s\n" "$idx" "$avg" >> "$RESULTS_FILE"
done

echo "? Per-query flat-SG averages in $RESULTS_FILE"
