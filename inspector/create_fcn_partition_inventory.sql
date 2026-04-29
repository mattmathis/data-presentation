# partition_inventory(tbl, start_date, end_date)
#
# Returns per-partition row counts and sizes for any date-partitioned table
# in mm_preproduction, between start_date and end_date inclusive.
#
# NOTE: INFORMATION_SCHEMA is dataset-scoped, so this function only sees
# tables in mlab-collaboration.mm_preproduction.
#
# Example:
#   SELECT * FROM mm_preproduction.partition_inventory(
#     'extended_intermediate_downloads_DS16', '2025-01-01', '2025-03-31')
#   ORDER BY partition_date;

CREATE OR REPLACE TABLE FUNCTION `mlab-collaboration.mm_preproduction.partition_inventory` (
  tbl        STRING,
  start_date DATE,
  end_date   DATE
) AS (
  SELECT
    PARSE_DATE('%Y%m%d', partition_id)        AS partition_date,
    total_rows,
    total_logical_bytes,
    ROUND(total_logical_bytes / POW(1024, 3), 3)   AS logical_GB,
    total_billable_bytes,
    ROUND(total_billable_bytes / POW(1024, 3), 3)  AS billable_GB,
    last_modified_time
  FROM `mlab-collaboration.mm_preproduction.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    table_name   = tbl
    AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
    AND partition_id BETWEEN FORMAT_DATE('%Y%m%d', start_date)
                         AND FORMAT_DATE('%Y%m%d', end_date)
  ORDER BY partition_date
);
