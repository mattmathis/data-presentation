# update_scamper2_DS16_incrementally
#
# DML script

# Reset the entire table EXPENSIVE to RECREATE
# DROP TABLE  `mlab-collaboration.mm_preproduction.scamper2_DS16`;

# Reset the tmp table - only needed after a schema change
# DROP TABLE  `mlab-collaboration.mm_preproduction.scamper2_tmp`;

/* FIRST TIME ONLY
CREATE OR REPLACE TABLE `mlab-collaboration.mm_preproduction.scamper2_DS16`
  PARTITION BY dateAS ( SELECT *,

    FROM `measurement-lab.ndt.scamper2`
    WHERE date = '2026-01-01'
      AND FARM_FINGERPRINT(raw.Metadata.UUID) & 0xF = 0
);
*/

DECLARE replace_day DATE;

-- find the partition which we want to replace
SET replace_day = (SELECT MAX(date) FROM `mlab-collaboration.mm_preproduction.scamper2_DS16` WHERE date >= '2026-01-01');

WHILE replace_day < current_date()-1 DO

SET replace_day = (replace_day + 1);

SELECT replace_day;

CREATE OR REPLACE TABLE `mlab-collaboration.mm_preproduction.scamper2_tmp`
  PARTITION BY dateAS ( SELECT *,

    FROM `measurement-lab.ndt.scamper2`
    WHERE date = replace_day
      AND FARM_FINGERPRINT(raw.Metadata.UUID) & 0xF = 0
);

BEGIN TRANSACTION;

-- delete the entire partition
DELETE FROM `mlab-collaboration.mm_preproduction.scamper2_DS16` WHERE date = replace_day; -- Should be noop

-- insert the new data into the same partition
INSERT INTO `mlab-collaboration.mm_preproduction.scamper2_DS16`
  SELECT * FROM `mlab-collaboration.mm_preproduction.scamper2_tmp`
  WHERE date = replace_day
;

COMMIT TRANSACTION;
END WHILE;
# SELECT ERROR('Not error: Done');
