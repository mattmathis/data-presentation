# Backfill_extended_intermediate_downloads_DS16_incrementally
#
# DML script — works BACKWARDS from the oldest existing partition to stop_date.
# Adjust stop_date before running.

# Reset the entire table EXPENSIVE to RECREATE
# DROP TABLE  `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16`;

# Reset the tmp table - only needed after a schema change
# DROP TABLE  `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16_tmp`;

DECLARE replace_day DATE;
DECLARE stop_date DATE DEFAULT '2024-01-01';  -- ADJUST: backfill will not go earlier than this date

-- start from the oldest existing partition and work backwards
SET replace_day = (SELECT MIN(date) FROM `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16` WHERE date >= stop_date);

WHILE replace_day > stop_date DO

SET replace_day = (replace_day - 1);

CREATE OR REPLACE TABLE `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16_tmp`
  PARTITION BY date
  CLUSTER BY isValidBest, ServerContinent, ServerSite
  OPTIONS (require_partition_filter = true)
AS ( SELECT *,
      server.Geo.ContinentCode AS ServerContinent,
      server.Site AS ServerSite,
    FROM `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16`(replace_day) # Caution function
);

BEGIN TRANSACTION;

-- delete the entire partition
DELETE FROM `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16` WHERE date = replace_day; -- Should be noop

-- insert the new data into the same partition
INSERT INTO `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16`
  SELECT * FROM `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16_tmp`
  WHERE date = replace_day
;

COMMIT TRANSACTION;
END WHILE;
# SELECT ERROR('Not error: Done');
