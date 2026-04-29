# Table inspection tools

## Functions

### [create_fcn_table_inventory.sql](create_fcn_table_inventory.sql)
- Creates table function `mm_preproduction.table_inventory(project_dataset STRING, tbl STRING, start_date DATE, end_date DATE)`
- FROM `region-us.INFORMATION_SCHEMA.PARTITIONS`
- Returns per-partition row counts and sizes for any date-partitioned table in any US-region dataset
- Note: `project_dataset` format is `'project.dataset'`; regional INFORMATION_SCHEMA bills more than dataset-scoped

### [Create_fcn_partition_inventory.sql](Create_fcn_partition_inventory.sql)
- Creates table function `mm_preproduction.partition_inventory(tbl STRING, start_date DATE, end_date DATE)`
- FROM `mlab-collaboration.mm_preproduction.INFORMATION_SCHEMA.PARTITIONS`
- Returns per-partition row counts and sizes (logical and billable GB) for any date-partitioned table in `mm_preproduction`
- Note: dataset-scoped — only sees tables in `mm_preproduction`

## Inventory and auditing

### [Inventory_downsampled_tables.sql](Inventory_downsampled_tables.sql)
- Ad-hoc query tool
- FROM `measurement-lab.ndt.ndt7`, `ndt7_dynamic`, `mm_preproduction.ndt7_DS16`, `mm_preproduction.autoload_DS16`
- Reports per-date row counts and sampling error for both DS16 tables vs their sources

### [Table_inspection_tools.sql](Table_inspection_tools.sql)
- Ad-hoc query tool
- FROM `measurement-lab.ndt.ndt7`, `mlab-staging.ndt.ndt7`, `mlab-sandbox.ndt.ndt7`
- Compares test counts and server coverage across oti/staging/sandbox fleets

### [benchmark_downsampled_tables.sql](benchmark_downsampled_tables.sql)
- Ad-hoc benchmark query
- Compares query cost of `mm_preproduction.extended_intermediate_downloads_DS16` vs full `measurement-lab.ndt.unified_downloads`

## Validation

### [validate_extended_intermediate_downloads_DS16.sql](validate_extended_intermediate_downloads_DS16.sql)
- Ad-hoc validation query
- Compares a downsampled slice of `mm_preproduction.extended_intermediate_downloads_DS16` against production `measurement-lab.ndt_intermediate.extended_ndt7_downloads`
- Reports schema compatibility and IsValidBest row counts for both sources

### [validate_extended_intermediate_uploads_DS16.sql](validate_extended_intermediate_uploads_DS16.sql)
- Ad-hoc validation query
- Compares `mm_preproduction.extended_intermediate_uploads_DS16` against production `measurement-lab.ndt_intermediate.extended_ndt7_uploads`
- Reports IsValidBest agreement; shows rows where the two sources disagree
