access_log = LOAD 'data_set/access_logs.csv' USING PigStorage(',') AS (AccessId:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);

access_by_owner = GROUP access_log BY WhatPage;

-- Count the total number of accesses to Facebook pages made by each owner
access_count = FOREACH access_by_owner GENERATE group AS page_owner, COUNT(access_log) AS total_accesses;

-- Count the number of distinct Facebook pages accessed
distinct_pages = FOREACH access_by_owner {
    unique_pages = DISTINCT access_log.ByWho;
    GENERATE group AS page_owner, COUNT(unique_pages) AS distinct_pages_accessed;
}

result = JOIN access_count BY page_owner, distinct_pages BY page_owner;

STORE result INTO 'e_output' USING PigStorage(',');
