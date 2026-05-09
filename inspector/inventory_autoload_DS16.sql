# Inventory autoload_DS16
# Compares row counts between measurement-lab.ndt.ndt7_dynamic and the 1/16 subsample per date.

WITH

base AS (
  SELECT date, COUNT(*) AS Base
  FROM `measurement-lab.ndt.ndt7_dynamic`
  WHERE date >= CURRENT_DATE() - 30
  GROUP BY date
),
ds16 AS (
  SELECT date, COUNT(*) AS DS16
  FROM `mlab-collaboration.mm_preproduction.autoload_DS16`
  WHERE date >= CURRENT_DATE() - 30
  GROUP BY date
),
report AS (
  SELECT *,
    IFNULL((Base/16 - DS16)/DS16, 1.0) AS DS16error
  FROM base
  LEFT JOIN ds16 USING (date)
  ORDER BY ABS(DS16error) DESC
)

SELECT * FROM report
