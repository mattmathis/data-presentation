# 20251122PUD update extended_intermediate_downloads_DS16 incrementally

# DML script

# Reset the entire table EXPENSIVE to RECREATE
# DROP TABLE  `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16`;

# Reset the tmp table - only needed after a schema change
# DROP TABLE  `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16_tmp`; 

/* FIRST TIME ONLY
# DROP TABLE `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16` ;
CREATE OR REPLACE TABLE `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16`
  PARTITION BY date
  CLUSTER BY isValidBest, ServerContinent, ServerSite
  OPTIONS (require_partition_filter = true)
AS ( SELECT *,
      server.Geo.ContinentCode AS ServerContinent,
      server.Site AS ServerSite,
    FROM `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16`('2025-01-01')  -- Fixed start date
 );
*/

DECLARE replace_day DATE;

-- find the partition which we want to replace
SET replace_day = (SELECT MAX(date) FROM `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16` WHERE date >= '2024-01-01');

WHILE replace_day < current_date()-1 DO

SET replace_day = (replace_day + 1);

CREATE OR REPLACE TABLE `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16_tmp` 
  PARTITION BY date
  CLUSTER BY isValidBest, ServerContinent, ServerSite
  OPTIONS (require_partition_filter = true)
AS ( SELECT *,
      server.Geo.ContinentCode AS ServerContinent,
      server.Site AS ServerSite,
    FROM `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16`(replace_day) # Caution function
    -- WHERE date = replace_day
      -- AND FARM_FINGERPRINT(a.UUID) & 0xF =  0
);

BEGIN TRANSACTION;

-- delete the entire partition 
DELETE FROM `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16` WHERE date = replace_day; -- Should be noop

-- insert the new data into the same partition in mytable
INSERT INTO `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16` 
  SELECT * FROM `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16_tmp`
  WHERE date = replace_day
;

COMMIT TRANSACTION;
END WHILE;
SELECT ERROR('Not error: Done');
