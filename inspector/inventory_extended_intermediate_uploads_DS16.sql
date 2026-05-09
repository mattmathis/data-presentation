# Inventory extended_intermediate_uploads_DS16
# Checks coverage and quality-filter yield per date.
# "Input" is the combined ndt7_DS16 + autoload_DS16 row count for the day.
# No simple ratio error applies here since this table is filtered, not just subsampled.

WITH

input AS (
  SELECT date, COUNT(*) AS InputRows
  FROM `mlab-collaboration.mm_preproduction.ndt7_DS16`
  WHERE date >= CURRENT_DATE() - 30
  GROUP BY date

  UNION ALL

  SELECT date, COUNT(*) AS InputRows
  FROM `mlab-collaboration.mm_preproduction.autoload_DS16`
  WHERE date >= CURRENT_DATE() - 30
  GROUP BY date
),
input_agg AS (
  SELECT date, SUM(InputRows) AS InputRows
  FROM input
  GROUP BY date
),
output AS (
  SELECT date,
    COUNT(*) AS OutputRows,
    COUNTIF(isValidBest) AS ValidRows
  FROM `mlab-collaboration.mm_preproduction.extended_intermediate_uploads_DS16`
  WHERE date >= CURRENT_DATE() - 30
  GROUP BY date
),
report AS (
  SELECT
    date,
    InputRows,
    OutputRows,
    ValidRows,
    IFNULL(OutputRows / InputRows, 0.0) AS UploadFraction,
    IFNULL(ValidRows / OutputRows, 0.0) AS ValidFraction,
    OutputRows IS NULL AS MissingOutput
  FROM input_agg
  LEFT JOIN output USING (date)
  ORDER BY date DESC
)

SELECT * FROM report
