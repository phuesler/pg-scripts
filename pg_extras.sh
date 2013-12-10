#!/bin/bash
#
# COMMON POSTGRES MAINTENANCE QUERIES STOLEN FROM HEROKUs PG-Extras
# https://github.com/heroku/heroku-pg-extras/blob/master/init.rb
# Available commands so fare
#
# * cache_hit
# * index_usage
# * total_index_size
# * index_size
# * unused_indices
# * seq_scans
# * bloat
# * vacuum_stats
# * extensions
# * outliers
#
#
# USAGE: ./pg_extras.sh DATABASE COMMAND


DATABASE=$1
COMMAND=$2

CACHE_HIT="SELECT
      'index hit rate' AS name,
      (sum(idx_blks_hit)) / sum(idx_blks_hit + idx_blks_read) AS ratio
    FROM pg_statio_user_indexes
    UNION ALL
    SELECT
     'cache hit rate' AS name,
      sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)) AS ratio
    FROM pg_statio_user_tables;

"

INDEX_USAGE="SELECT relname,
     CASE idx_scan
       WHEN 0 THEN 'Insufficient data'
       ELSE (100 * idx_scan / (seq_scan + idx_scan))::text
     END percent_of_times_index_used,
     n_live_tup rows_in_table
   FROM
     pg_stat_user_tables
   ORDER BY
     n_live_tup DESC;
"

TOTAL_INDEX_SIZE="
SELECT pg_size_pretty(sum(relpages::bigint*8192)::bigint) AS size
      FROM pg_class
      WHERE reltype = 0;
"

INDEX_SIZE="
    SELECT relname AS name,
        pg_size_pretty(sum(relpages::bigint*8192)::bigint) AS size
      FROM pg_class
      WHERE reltype = 0
      GROUP BY relname
      ORDER BY sum(relpages) DESC;
"

UNUSED_INDICES="
      SELECT
        schemaname || '.' || relname AS table,
        indexrelname AS index,
        pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size,
        idx_scan as index_scans
      FROM pg_stat_user_indexes ui
      JOIN pg_index i ON ui.indexrelid = i.indexrelid
      WHERE NOT indisunique AND idx_scan < 50 AND pg_relation_size(relid) > 5 * 8192
      ORDER BY pg_relation_size(i.indexrelid) / nullif(idx_scan, 0) DESC NULLS FIRST,
      pg_relation_size(i.indexrelid) DESC;
"

SEQ_SCANS="SELECT relname AS name,
             seq_scan as count
      FROM
        pg_stat_user_tables
      ORDER BY seq_scan DESC;
"

BLOAT="WITH constants AS (
          SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 4 AS ma
        ), bloat_info AS (
          SELECT
            ma,bs,schemaname,tablename,
            (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
            (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
          FROM (
            SELECT
              schemaname, tablename, hdr, ma, bs,
              SUM((1-null_frac)*avg_width) AS datawidth,
              MAX(null_frac) AS maxfracsum,
              hdr+(
                SELECT 1+count(*)/8
                FROM pg_stats s2
                WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
              ) AS nullhdr
            FROM pg_stats s, constants
            GROUP BY 1,2,3,4,5
          ) AS foo
        ), table_bloat AS (
          SELECT
            schemaname, tablename, cc.relpages, bs,
            CEIL((cc.reltuples*((datahdr+ma-
              (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta
          FROM bloat_info
          JOIN pg_class cc ON cc.relname = bloat_info.tablename
          JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = bloat_info.schemaname AND nn.nspname <> 'information_schema'
        ), index_bloat AS (
          SELECT
            schemaname, tablename, bs,
            COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
            COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
          FROM bloat_info
          JOIN pg_class cc ON cc.relname = bloat_info.tablename
          JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = bloat_info.schemaname AND nn.nspname <> 'information_schema'
          JOIN pg_index i ON indrelid = cc.oid
          JOIN pg_class c2 ON c2.oid = i.indexrelid
        )
        SELECT
          type, schemaname, object_name, bloat, pg_size_pretty(raw_waste) as waste
        FROM
        (SELECT
          'table' as type,
          schemaname,
          tablename as object_name,
          ROUND(CASE WHEN otta=0 THEN 0.0 ELSE table_bloat.relpages/otta::numeric END,1) AS bloat,
          CASE WHEN relpages < otta THEN '0' ELSE (bs*(table_bloat.relpages-otta)::bigint)::bigint END AS raw_waste
        FROM
          table_bloat
            UNION
        SELECT
          'index' as type,
          schemaname,
          tablename || '::' || iname as object_name,
          ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS bloat,
          CASE WHEN ipages < iotta THEN '0' ELSE (bs*(ipages-iotta))::bigint END AS raw_waste
        FROM
          index_bloat) bloat_summary
        ORDER BY raw_waste DESC, bloat DESC
"

VACUUM_STATS="
WITH table_opts AS (
        SELECT
          pg_class.oid, relname, nspname, array_to_string(reloptions, '') AS relopts
        FROM
           pg_class INNER JOIN pg_namespace ns ON relnamespace = ns.oid
      ), vacuum_settings AS (
        SELECT
          oid, relname, nspname,
          CASE
            WHEN relopts LIKE '%autovacuum_vacuum_threshold%'
              THEN regexp_replace(relopts, '.*autovacuum_vacuum_threshold=([0-9.]+).*', E'\\\\\\1')::integer
              ELSE current_setting('autovacuum_vacuum_threshold')::integer
            END AS autovacuum_vacuum_threshold,
          CASE
            WHEN relopts LIKE '%autovacuum_vacuum_scale_factor%'
              THEN regexp_replace(relopts, '.*autovacuum_vacuum_scale_factor=([0-9.]+).*', E'\\\\\\1')::real
              ELSE current_setting('autovacuum_vacuum_scale_factor')::real
            END AS autovacuum_vacuum_scale_factor
        FROM
          table_opts
      )
      SELECT
        vacuum_settings.nspname AS schema,
        vacuum_settings.relname AS table,
        to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') AS last_vacuum,
        to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') AS last_autovacuum,
        to_char(pg_class.reltuples, '9G999G999G999') AS rowcount,
        to_char(psut.n_dead_tup, '9G999G999G999') AS dead_rowcount,
        to_char(autovacuum_vacuum_threshold
             + (autovacuum_vacuum_scale_factor::numeric * pg_class.reltuples), '9G999G999G999') AS autovacuum_threshold,
        CASE
          WHEN autovacuum_vacuum_threshold + (autovacuum_vacuum_scale_factor::numeric * pg_class.reltuples) < psut.n_dead_tup
          THEN 'yes'
        END AS expect_autovacuum
      FROM
        pg_stat_user_tables psut INNER JOIN pg_class ON psut.relid = pg_class.oid
          INNER JOIN vacuum_settings ON pg_class.oid = vacuum_settings.oid
      ORDER BY 1
"

EXTENSIONS="SELECT * FROM pg_available_extensions WHERE name IN (SELECT unnest(string_to_array(current_setting('extwlist.extensions'), ',')))"
OUTLIERS="SELECT query, time, calls, hits
  FROM (
    SELECT query, (total_time/calls) AS time, calls,
           AVG(calls) OVER () AS avg_calls,
           AVG(total_time/calls) OVER () AS avg_time,
           100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hits
    FROM pg_stat_statements WHERE userid = (SELECT usesysid FROM pg_user WHERE usename = current_user LIMIT 1)
  ) AS ss
  WHERE calls > avg_calls AND time > avg_time
  ORDER BY hits ASC, calls DESC, avg_time ASC
"

case "$COMMAND" in
  index_usage)
      QUERY=$INDEX_USGAGE
      ;;
  cache_hit)
      QUERY=$CACHE_HIT
      ;;
  total_index_size)
    QUERY=$TOTAL_INDEX_SIZE
  ;;
  index_size)
    QUERY=$INDEX_SIZE
  ;;
  unused_indices)
    QUERY=$UNUSED_INDICES
  ;;
  seq_scanes)
    QUERY=$SEQ_SCANS
  ;;
  bloat)
    QUERY=$BLOAT
  ;;
  vacuum_stats)
    QUERY=$VACUUM_STATS
  ;;
  extensions)
    QUERY=$EXTENSIONS
  ;;
  outliers)
    QUERY=$OUTLIERS
  ;;
esac


psql -U postgres -d $1 -c "$QUERY"
