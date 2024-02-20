-- Load the access logs
access_logs = LOAD 'data_set/access_logs.csv' USING PigStorage(',') AS (AccessID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:chararray);

-- Load the page logs
pages = LOAD 'data_set/pages.csv' USING PigStorage(',') AS (PersonID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

filtered_logs = FILTER access_logs BY AccessTime >= '2022-02-02';
joined_data = JOIN filtered_logs BY ByWho, pages BY PersonID;

-- Project the required fields (PersonID and Name)
result = FOREACH joined_data GENERATE pages::PersonID, pages::Name;

result = DISTINCT result;

dump result;
STORE final_output INTO 'g_output' USING PigStorage(',');
