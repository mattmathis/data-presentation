# 20251122PUD benchmark downsampled tables

# Comment out unused cases to prevent any compile time effects

/*
# Unified downloads for europe only
# 31 day '2025-10-01' AND '2025-10-31'
# Estimate 1.47 TB
# Actual 1.47 TB 32082316 rows
# One day '2025-11-01'
# Result: processed 46.3 GB for 109681 rows counted
SELECT
  COUNT (*)
FROM `measurement-lab.ndt.unified_downloads`
WHERE
  DATE between '2025-10-01' AND '2025-10-31'
  AND server.geo.continentCode = 'EU'
  AND DATE(a.testTime) < CURRENT_date()  -- Prevent caching
*/


# 31 day downsampled downloads '2025-10-01' AND '2025-10-31'
# Estimates 49.85 MB
# Actual: 49.85 MB (50 MB billed) 2160520 rows
# 
SELECT
  COUNT (*)
FROM `mlab-collaboration.mm_preproduction.extended_intermediate_downloads_DS16`
WHERE
  DATE between '2025-10-01' AND '2025-10-31'
  AND ServerContinent = 'EU'
  AND DATE(a.testTime) < CURRENT_date()  -- Prevent caching
