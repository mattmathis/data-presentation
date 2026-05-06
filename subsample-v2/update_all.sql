-- config { type: "table" }

-- Incrementally update all DS16 tables
-- To be run at daily at 23:00 UTC

CALL update_extended_intermediate_downloads_DS16_incrementally.sql;

CALL update_extended_intermediate_uploads_DS16_incrementally.sql;

CALL Update_ndt7_DS16_incrementally.sql;

CALL Update_ndt7_autoload_incrementally.sql;

CALL update_scamper1_DS16_incrementally.sql;

CALL update_scamper2_DS16_incrementally.sql;
