# 20251122PUD validate extended_intermediate_uploads_DS16

# Validate a new materiazed version of unified_uploads
# Note that this should be in a colab or other scripting facility

WITH

############ Precondition Production data for comparison
# align the schema
sanitizeProd AS (
  SELECT id, date, a,
  STRUCT (
    metadata.View,
    metadata.Protocol,
    metadata.ClientMetadata,
    metadata.ServerMetadata,
    [ 'Fake ArchiveURL' ] AS ArchiveURL
  ) AS metadata,
  STRUCT (
    filter.IsComplete,
    filter.IsProduction,
    filter.IsError,
    filter.IsOAM,
    filter.IsPlatformAnomaly,
    filter.IsSmall,
    filter.IsShort,
    filter.IsLong,
    filter.IsEarlyExit
  ) AS filter,
  client, server
  FROM `measurement-lab.ndt_intermediate.extended_ndt7_uploads`  -- PRODUCTION 
),
sanitizedUnified AS (  -- Only adds isValidBest
  SELECT *,
  (
      filter.IsComplete -- Not missing any important fields
      AND filter.IsProduction -- not a test server
      AND NOT filter.IsError -- Server reported an error
      AND NOT filter.IsOAM -- operations and management traffic
      AND NOT filter.IsPlatformAnomaly -- overload, bad version, etc
      AND NOT filter.IsSmall -- less than 8kB data
      AND (NOT filter.IsShort OR filter.IsEarlyExit) -- insufficient duration or early exit.
      AND NOT filter.IsLong -- excessive duraton
      -- TODO(https://github.com/m-lab/k8s-support/issues/668) deprecate? _IsRFC1918
      -- AND NOT filter._IsRFC1918
    ) AS IsValidBest,
  FROM sanitizeProd
),

# Get a sip of Production Data
SampledProd AS (
  SELECT
    'Prod' AS src, *
    FROM sanitizedUnified
    WHERE
      date = '2025-11-01'  -- One test Day
      AND FARM_FINGERPRINT(a.UUID) & 0xF =  0  -- DOWNSAMPLE
),

############ Precondition downsampled data
# _fleet is a workaround for problems with the metadata
sanitizeTestFcn AS (
  SELECT * EXCEPT ( _internal202511, _Fleet )
  # Caution, table and function have the same name - this is the function
  FROM `mlab-collaboration.mm_preproduction.extended_intermediate_uploads_DS16`("2025-11-01")
),

SampledTestData AS (
  SELECT
    'Test' AS src, * EXCEPT ( ServerContinent, ServerSite, _internal202511, _fleet)
    # Caution, table and function have the same name - this is the table
    FROM `mlab-collaboration.mm_preproduction.extended_intermediate_uploads_DS16`
    WHERE
      date = '2025-11-01'  -- One test Day
      AND _fleet = 'legacy' -- Autojoin data can't be tested because is not in the production tables
),

############ Comparison Sub Querys

# This is a COMPILE TIME test to validate schemas before building the table.
# NB: the downsampled side tests function that creates the table
PreCache AS (
  ( SELECT * FROM sanitizedUnified
    WHERE date = '2009-01-01')  -- Zero rows
                UNION ALL
  ( SELECT * FROM sanitizeTestFcn
    WHERE date = '2009-01-01')  -- Zero rows
),

# Diagnotic Union of two tables, coereced to be the same
MatchedSamples AS (
  (SELECT * FROM SampledProd )
            UNION ALL
  (SELECT * FROM SampledTestData )   
),
inventory AS (  -- Count IsValidBest
  SELECT src, COUNT(*) AS totalRows, COUNTIF(IsValidBest) AS isValid
  FROM MatchedSamples
  GROUP BY src
),
JoinedSamples AS (
  SELECT id, L, R
  FROM SampledProd AS L JOIN SampledTestData AS R USING (id)
),
Problems AS ( -- Rows that differ between sources
  SELECT id
  FROM JoinedSamples
  WHERE L.IsValidBest != R.IsValidBest
  # Add additional tests here
),
ShowProblems AS (
  SELECT
    L.IsValidBest AS LiVN, L.filter AS Lf,
    R.IsValidBest AS RiVN, R.filter AS Rf
  FROM JoinedSamples JOIN Problems USING (id)
)

SELECT * FROM inventory
# SELECT * FROM ShowProblems LIMIT 1000
