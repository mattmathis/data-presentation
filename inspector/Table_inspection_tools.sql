
# 20250323MV 1116 Table inspection tools

# Charterize the differences between mlab-oti, mlab-staging and mlab-sandbox version of ndt.ndt7

WITH

ndt_oti AS (
  SELECT FORMAT('%t-%t',server.Site, Server.Machine) AS serv, *
  # FROM `mlab-oti.ndt.ndt7` -- I don't have permission for direct access
  FROM `measurement-lab.ndt.ndt7` -- Via a pass through
  WHERE date BETWEEN '2025-11-01' AND '2025-11-10'
),

ndt_staging AS (
  SELECT FORMAT('%t-%t',server.Site, Server.Machine) AS serv, *
  FROM `mlab-staging.ndt.ndt7`
  WHERE date BETWEEN '2025-11-01' AND '2025-11-10'
),

ndt_sandbox AS (
  SELECT FORMAT('%t-%t',server.Site, Server.Machine) AS serv, *
  FROM `mlab-sandbox.ndt.ndt7`
  WHERE date BETWEEN '2025-11-01' AND '2025-11-10'
),

test1 AS (  -- inventory tests and servers per per table 
  SELECT
    COUNT (*) AS tests,
    COUNT (DISTINCT serv) AS servers
  # FROM ndt_oti -- test1a 60501906 tests 374 servers, 1.24 GB billed
  FROM ndt_staging -- test1b 60502018 tests 374 servers, 1.24 GB billed (~100 more tests)
  # FROM ndt_sandbox -- test1c: 264 tests from 9 servers
),

test2 AS ( -- SHow server names and test counts
  SELECT serv, COUNT (*) AS tests FROM ndt_sandbox GROUP BY serv ORDER BY serv
),
# We see lga0t-*, lga1t-*, chs0t-mlab2 and geg01-mlab4

test3 AS (
  SELECT serv, COUNT (*) AS tests
  -- FROM ndt_sandbox -- testing
  FROM ndt_staging
  WHERE REGEXP_CONTAINS(serv, '^[a-z]{3}.t') OR REGEXP_CONTAINS(serv, 'mlab4')
  GROUP BY serv
),
# The staging table only has production data, and no data from the staging fleet

test4 AS (
  SELECT
    L.serv, L.a.TestTIme, R.serv, R.a.Testtime
  FROM ndt_oti AS L FULL OUTER JOIN ndt_staging AS R USING (id)
  WHERE L.serv IS NULL OR R.serv IS NULL
  ORDER BY R.a.Testtime
)
# Missing rowa are 112 close timestapms from mil05-mlab2 
# between 2025-11-04 00:00:10.159187 UTC
#     and 2025-11-04 00:15:28.399896 UTC

SELECT * FROM test4



