# create_fcn_table_inventory(project_dataset, tbl, start_date, end_date)
# This is not useable, because it requires to many permissions
#
# Returns per-partition row counts and sizes for any date-partitioned table
# in any dataset within the US region, between start_date and end_date inclusive.
#
# project_dataset: 'project.dataset', e.g. 'mlab-collaboration.mm_preproduction'
#
# Uses region-us.INFORMATION_SCHEMA.PARTITIONS (covers all datasets in the US region).
# NOTE: querying the regional INFORMATION_SCHEMA bills more than the dataset-scoped version.
#
# Example:
#   SELECT * FROM mm_preproduction.table_inventory(
#     'mlab-collaboration.mm_preproduction',
#     'extended_intermediate_downloads_DS16', '2025-01-01', '2025-03-31')
#   ORDER BY partition_date;

CREATE OR REPLACE TABLE FUNCTION `mlab-collaboration.mm_preproduction.table_inventory` (
  project_dataset STRING,
  tbl             STRING,
  start_date      DATE,
  end_date        DATE
) AS (
  SELECT
    PARSE_DATE('%Y%m%d', partition_id)             AS partition_date,
    total_rows,
    total_logical_bytes,
    ROUND(total_logical_bytes / POW(1024, 3), 3)   AS logical_GB,
    total_billable_bytes,
    ROUND(total_billable_bytes / POW(1024, 3), 3)  AS billable_GB,
    last_modified_time
  FROM `region-us.INFORMATION_SCHEMA.PARTITIONS`
  WHERE
    table_catalog = SPLIT(project_dataset, '.')[OFFSET(0)]
    AND table_schema  = SPLIT(project_dataset, '.')[OFFSET(1)]
    AND table_name    = tbl
    AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
    AND partition_id BETWEEN FORMAT_DATE('%Y%m%d', start_date)
                         AND FORMAT_DATE('%Y%m%d', end_date)
  ORDER BY partition_date
);
