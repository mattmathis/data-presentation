# 20251122PUD update extended_intermediate_downloads_DS1C incrementally

# DML script

DECLARE replace_day, startDate, endDate DATE;

SET startDate = '2024-11-25';
SET endDate = '2024-11-30';

# This is similiar to other subsampling scripts, however it is a strict 1:1 copy of _DS1V
# Which is itself a manually constructed view, and is not a true subsample.
# Note that _DS1V already included the clustering columns

# Reset the entire table EXPENSIVE to RECREATE
# DROP TABLE  `mm_preproduction.extended_intermediate_downloads_DS1C`;

# Reset the tmp table - only needed after a schema change
# DROP TABLE  `mm_preproduction.extended_intermediate_downloads_DS1C_tmp`; 

/* FIRST TIME ONLY

CREATE OR REPLACE TABLE `mm_preproduction.extended_intermediate_downloads_DS1C` 
  PARTITION BY date
  CLUSTER BY isValidBest, ServerContinent, ServerSite
  OPTIONS (require_partition_filter = true)
AS ( SELECT *,
    FROM `mm_preproduction.extended_intermediate_downloads_DS1V` 
    WHERE date = '2025-03-01'
      AND FARM_FINGERPRINT(a.UUID) & 0xF =  0
);
*/


# Compile time schema validation, using downloads_DS16
SELECT * FROM `mm_preproduction.extended_intermediate_downloads_DS1C`
WHERE date = '2008-01-01' -- Out of range
  UNION ALL
( SELECT * FROM `mm_preproduction.extended_intermediate_downloads_DS16`
  WHERE date = '2008-01-01' -- Out of range
);

-- find the partition which we want to replace
SET replace_day = startDate;

WHILE replace_day <= endDate DO

CREATE OR REPLACE TABLE `mm_preproduction.extended_intermediate_downloads_DS1C_tmp` 
  PARTITION BY date
  CLUSTER BY isValidBest, ServerContinent, ServerSite
  OPTIONS (require_partition_filter = true)
AS ( SELECT *,
    FROM `mm_preproduction.extended_intermediate_downloads_DS1V`
    WHERE date = replace_day
);

BEGIN TRANSACTION;

-- delete the entire partition 
DELETE FROM `mm_preproduction.extended_intermediate_downloads_DS1C` WHERE date = replace_day; -- Usually a noop

-- insert the new data into the same partition in mytable
INSERT INTO `mm_preproduction.extended_intermediate_downloads_DS1C` 
  SELECT * FROM `mm_preproduction.extended_intermediate_downloads_DS1C_tmp`
  WHERE date = replace_day
;

COMMIT TRANSACTION;

SET replace_day = (replace_day + 1);

END WHILE;
# SELECT ERROR('Not error: Done');


