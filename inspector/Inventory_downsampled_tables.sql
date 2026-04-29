# 20250323MV 0418 Inventory downsampled tables

WITH

invNdt7Base AS (
  SELECT date, COUNT(*) AS Base
  FROM `measurement-lab.ndt.ndt7` 
  WHERE date >= '2025-01-01'
  GROUP BY date
),
invAutoloadBase AS (
  SELECT date, COUNT(*) AS AlBase
  FROM `measurement-lab.ndt.ndt7_dynamic` 
  WHERE date >= '2025-01-01'
  GROUP BY date
),
invNdt7DS16k AS (
  SELECT date, COUNT(*) AS DS16
  FROM `mlab-collaboration.mm_preproduction.ndt7_DS16` 
  WHERE date >= '2025-01-01'
  GROUP BY date
),
 invAutoloadDS16k AS (
  SELECT date, COUNT(*) AS AlDS16
  FROM `mlab-collaboration.mm_preproduction.autoload_DS16` 
  WHERE date >= '2025-01-01'
  GROUP BY date
),
report AS (
  SELECT *,
  (Base/16 - DS16)/DS16 AS DS16error,
  (AlBase/16 - AlDS16)/AlDS16 AS AlDS16error,
  FROM (
    SELECT * FROM invNdt7Base
      LEFT JOIN invNdt7DS16k USING ( date )
      LEFT JOIN invautoloadBase USING ( date )
      LEFT JOIN invautoloadDS16k USING ( date )
  )
  # ORDER BY ABS(error) DESC
  ORDER BY date desc
)

SELECT * FROM report 
# SELECT * FROM invNdt7DS16k ORDER BY date
