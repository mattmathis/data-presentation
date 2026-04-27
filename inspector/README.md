# Table inspection tools

## Functions

### [partition_inventory.sql](partition_inventory.sql)
- Creates table function `mm_preproduction.partition_inventory(tbl STRING, start_date DATE, end_date DATE)`
- FROM `mlab-collaboration.mm_preproduction.INFORMATION_SCHEMA.PARTITIONS`
- Returns per-partition row counts and sizes (logical and billable GB) for any date-partitioned table in `mm_preproduction`
- Note: dataset-scoped — only sees tables in `mm_preproduction`
