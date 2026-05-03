# 20250323MV 0418 Inventory downsampled tables

# Warning: this is expensive (> 8.8 TiB).

# TODO:
# Exclude tiny or null autojoin data
# Counts for mm_preproduction.autoload_DS16 are off for 2026-10-09 thru 16

WITH

invNdt7Base AS (
  SELECT date, COUNT(*) AS Base
  FROM `measurement-lab.ndt.ndt7` 
  WHERE date >= '2024-01-01'
  GROUP BY date
),
invAutoloadBase AS (
  SELECT date, COUNT(*) AS AlBase
  FROM `measurement-lab.ndt.ndt7_dynamic` 
  WHERE date >= '2024-01-01'
  GROUP BY date
),
invNdt7DS16k AS (
  SELECT date, COUNT(*) AS DS16
  FROM `mlab-collaboration.mm_preproduction.ndt7_DS16` 
  WHERE date >= '2024-01-01'
  GROUP BY date
),
 invAutoloadDS16k AS (
  SELECT date, COUNT(*) AS AlDS16
  FROM `mlab-collaboration.mm_preproduction.autoload_DS16` 
  WHERE date >= '2024-01-01'
  GROUP BY date
),
report AS (
  SELECT *,
  -- Any missing data shows the maximum error
  IFNULL((Base/16 - DS16)/DS16, 1.0) AS DS16error,
  IFNULL((AlBase/16 - AlDS16)/AlDS16, 1.0) AS AlDS16error,
  FROM (
    SELECT * FROM invNdt7Base
      LEFT JOIN invNdt7DS16k USING ( date )
      LEFT JOIN invautoloadBase USING ( date )
      LEFT JOIN invautoloadDS16k USING ( date )
  )
  # Find all outliers 
  ORDER BY GREATEST(ABS(DS16error), ABS(AlDS16error)) DESC
  # ORDER BY date desc
)

SELECT * FROM report 
# SELECT * FROM invNdt7DS16k ORDER BY date
