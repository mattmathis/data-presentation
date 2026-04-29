# First generation subsample tools

As these scripts are migrated or discarded, mark the entries here and move them to the bottom of the file

## Raw NDT7 subsamples (was 20250323MV)

### [Update_ndt7_DS16_incrementally.sql](Update_ndt7_DS16_incrementally.sql)
- FROM `measurement-lab.ndt.ndt7`
- Pure subsample INTO `mlab-collaboration.mm_preproduction.ndt7_DS16`
- Scheduled 2200 UTC daily

### [Backfill_ndt7_DS16_incrementally.sql](Backfill_ndt7_DS16_incrementally.sql)
- FROM `measurement-lab.ndt.ndt7`
- Pure subsample INTO `mlab-collaboration.mm_preproduction.ndt7_DS16`
- Backfill variant of the incremental update

### [Update_autoload_DS16_incrementally.sql](Update_autoload_DS16_incrementally.sql)
- FROM `measurement-lab.ndt.ndt7_dynamic`
- Pure subsample INTO `mlab-collaboration.mm_preproduction.autoload_DS16`
- Scheduled 2200 UTC daily

### [Backfill_autoload_DS16_incrementally.sql](Backfill_autoload_DS16_incrementally.sql)
- FROM `measurement-lab.ndt.ndt7_dynamic`
- Pure subsample INTO `mlab-collaboration.mm_preproduction.autoload_DS16`
- Backfill variant of the incremental update

### [update_scamper1_DS16_incrementally.sql](update_scamper1_DS16_incrementally.sql)
- FROM `measurement-lab.ndt.scamper1`
- Pure subsample (1:16 on `raw.Metadata.UUID`) INTO `mlab-collaboration.mm_preproduction.scamper1_DS16`
- Scheduled daily; starts from 2026-01-01

### [Backfill_scamper1_DS16_incrementally.sql](Backfill_scamper1_DS16_incrementally.sql)
- FROM `measurement-lab.ndt.scamper1`
- Pure subsample INTO `mlab-collaboration.mm_preproduction.scamper1_DS16`
- Backfill variant; stop_date default 2026-01-01

### [update_scamper2_DS16_incrementally.sql](update_scamper2_DS16_incrementally.sql)
- FROM `measurement-lab.ndt.scamper2`
- Pure subsample (1:16 on `raw.Metadata.UUID`) INTO `mlab-collaboration.mm_preproduction.scamper2_DS16`
- Scheduled daily; starts from 2026-01-01

### [Backfill_scamper2_DS16_incrementally.sql](Backfill_scamper2_DS16_incrementally.sql)
- FROM `measurement-lab.ndt.scamper2`
- Pure subsample INTO `mlab-collaboration.mm_preproduction.scamper2_DS16`
- Backfill variant; stop_date default 2026-01-01

---

## Extended intermediate tables (was 20251122PUD)

Downsampled replacements for production views `ndt_intermediate.extended_ndt7_downloads` and `extended_ndt7_uploads`.

### [create_fcn_extended_intermediate_downloads_DS16.sql](create_fcn_extended_intermediate_downloads_DS16.sql)
- Creates table function `mm_preproduction.extended_intermediate_downloads_DS16(start DATE)`
- FROM `mm_preproduction.ndt7_DS16` UNION `mm_preproduction.autoload_DS16`
- Applies download filtering, quality flags, and unified schema matching production `extended_ndt7_downloads`

### [create_fcn_extended_intermediate_uploads_DS16.sql](create_fcn_extended_intermediate_uploads_DS16.sql)
- Creates table function `mm_preproduction.extended_intermediate_uploads_DS16(start DATE)`
- FROM `mm_preproduction.ndt7_DS16` UNION `mm_preproduction.autoload_DS16`
- Applies upload filtering, quality flags, and unified schema matching production `extended_ndt7_uploads`

### [update_extended_intermediate_downloads_DS16_incrementally.sql](update_extended_intermediate_downloads_DS16_incrementally.sql)
- DML loop: advances from MAX(date) in table up to yesterday, one partition at a time
- Calls the downloads table function per day; uses a tmp table + transaction
- INTO `mm_preproduction.extended_intermediate_downloads_DS16`

### [Backfill_extended_intermediate_downloads_DS16_incrementally.sql](Backfill_extended_intermediate_downloads_DS16_incrementally.sql)
- DML loop: works backwards from MIN(date) in table to stop_date (default 2024-01-01)
- Calls the downloads table function per day; uses a tmp table + transaction
- INTO `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16`

### [update_extended_intermediate_downloads_DS1C_incrementally.sql](update_extended_intermediate_downloads_DS1C_incrementally.sql)
- DML loop: copies a fixed date range from `extended_intermediate_downloads_DS1V`
- This manually constructed table was used to mask schema differences
to validate subsampled tables against production tables
- INTO `mm_preproduction.extended_intermediate_downloads_DS1C`

### [update_extended_intermediate_uploads_DS16_incrementally.sql](update_extended_intermediate_uploads_DS16_incrementally.sql)
- DML loop: advances from MAX(date) in table up to yesterday, one partition at a time
- Calls the uploads table function per day; uses a tmp table + travalidate subsampled tables against nsaction
- INTO `mm_preproduction.extended_intermediate_uploads_DS16`

### [Backfill_extended_intermediate_uploads_DS16_incrementally.sql](Backfill_extended_intermediate_uploads_DS16_incrementally.sql)
- DML loop: works backwards from MIN(date) in table to stop_date (default 2024-01-01)
- Calls the uploads table function per day; uses a tmp table + transaction
- INTO `mlab-collaboration.mm_preproduction.extended_intermediate_uploads_DS16`

---

## Ported or fully deprecated code

### 20250323MV_0321_Create_downsampled_materialized_views.sql
- **STOP: obsolete** — materialized views get recreated every 3 hours
- FROM `mlab-autojoin.autoload_v2_ndt.ndt7_union`
- Pure subsample (1:1000) INTO `mm_preproduction.ndt7_autoload_union_DS1k`

### 20250323MV_0323_Create_downsampled_sharded_tables.sql
- **STOP: destructive** — drops and recreates the table
- FROM `measurement-lab.ndt.ndt7`
- Pure subsample (1:16) INTO `mm_preproduction.ndt7_DS16`; one-time creation script

### 20250323MV_0805_Update_ndt7_autoload_union_DS16_incrementally.sql
- **STOP: marked obsolete**
- FROM `mlab-autojoin.autoload_v2_ndt.ndt7_union`
- Pure subsample INTO `mlab-collaboration.mm_preproduction.ndt7_autoload_union_DS16`
