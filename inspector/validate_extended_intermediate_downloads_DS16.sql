# 20251122PUD validate extended_intermediate_downloads_DS16

# Validate a new materiazed version of unified_downloads
# Note that this should be in a colab or other scripting facility

WITH

# Align old schema with the new
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
    filter.isComplete,
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
  FROM `measurement-lab.ndt_intermediate.extended_ndt7_downloads`  -- PRODUCTION 
),
sanatizedUnified AS (  -- Only adds isValidBest
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

# Remove know to be problematic fields, most are expected
# metadata is due to a schema BUG20251122PUD
# _fleet is a workaround for problems with the metadata
sanitizeTestFcn AS (
  SELECT * EXCEPT ( _internal202511, _fleet)
  FROM `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16`("2025-11-01")
),

# This is a compile time test to validate schemas
# before building the table.
# no point in trying to run it
PreCache AS (
  ( SELECT * FROM sanatizedUnified
    WHERE date = '2009-01-01')  -- Zero rows
                UNION ALL
  ( SELECT * FROM sanitizeTestFcn
    WHERE date = '2009-01-01')  -- Zero rows
),

# Diagnotic Union of two tables, coereced to be the same
MatchedSamples AS (
  ( SELECT
    'Prod' AS src, *
    # FROM `measurement-lab.ndt_intermediate.extended_ndt7_downloads`  -- PRODUCTION 
    FROM sanatizedUnified
    WHERE
      date = '2025-11-01'  -- One test Day
      AND FARM_FINGERPRINT(a.UUID) & 0xF =  0  -- DOWNSAMPLE
  )
            UNION ALL

  ( SELECT
    'Test' AS src, * EXCEPT ( ServerContinent, ServerSite, _internal202511, _fleet)
    FROM `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16`  -- Materialized table
    WHERE
      date = '2025-11-01'  -- One test Day
      AND _fleet = 'legacy'  -- Autojoin data can't be tested
  )
),

inventory AS (
  SELECT src, COUNT(*) AS totalRows, COUNTIF(isValidBest) AS isValid
  FROM MatchedSamples
  GROUP BY src
)

SELECT * FROM inventory