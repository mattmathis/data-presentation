# Backfill_DS16_incrementally
#
# DML script — works BACKWARDS from the oldest existing partition to stop_date.
# Adjust stop_date before running.

# Reset the entire table EXPENSIVE to RECREATE
# DROP TABLE  `mlab-collaboration.mm_preproduction.autoload_DS16`;

# Reset the tmp table - only needed after a schema change
# DROP TABLE  `mlab-collaboration.mm_preproduction.autoload_tmp`;

/* FIRST TIME ONLY
CREATE OR REPLACE TABLE `mlab-collaboration.mm_preproduction.autoload_DS16`
  PARTITION BY date
  CLUSTER BY Continent, ServerSite
AS ( SELECT *,
      server.Geo.ContinentCode AS Continent,
      server.Site AS ServerSite,
    FROM `measurement-lab.ndt.ndt7_dynamic`
    WHERE date = '2025-03-01'
      AND FARM_FINGERPRINT(a.UUID) & 0xF =  0
);
*/

DECLARE replace_day DATE;
DECLARE stop_date DATE DEFAULT '2020-01-01';  -- ADJUST: backfill will not go earlier than this date

-- start from the oldest existing partition and work backwards
SET replace_day = (SELECT MIN(date) FROM `mlab-collaboration.mm_preproduction.autoload_DS16`);

WHILE replace_day > stop_date DO

SET replace_day = (replace_day - 1);

CREATE OR REPLACE TABLE `mlab-collaboration.mm_preproduction.autoload_tmp`
  PARTITION BY date
  CLUSTER BY Continent, ServerSite
AS ( SELECT *,
      server.Geo.ContinentCode AS Continent,
      server.Site AS ServerSite,
    FROM `measurement-lab.ndt.ndt7_dynamic`
    WHERE date = replace_day
      AND FARM_FINGERPRINT(a.UUID) & 0xF =  0
);

BEGIN TRANSACTION;

-- delete the entire partition
DELETE FROM `mlab-collaboration.mm_preproduction.autoload_DS16` WHERE date = replace_day; -- Should be noop

-- insert the new data into the same partition
INSERT INTO `mlab-collaboration.mm_preproduction.autoload_DS16`
  SELECT * FROM `mlab-collaboration.mm_preproduction.autoload_tmp`
  WHERE date = replace_day
;

COMMIT TRANSACTION;
END WHILE;
# SELECT ERROR('Not error: Done');
