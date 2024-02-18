pages = LOAD 'data_set/pages.csv' USING PigStorage(',') AS (personID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

access_log = LOAD 'data_set/access_log.csv' USING PigStorage(',') AS (AccessId:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:int);

current_time = GetUnixTime();
fourteen_days_ago = current_time - (14 * 24 * 3600);

recent_access_log = FILTER access_log BY AccessTime >= fourteen_days_ago;
access_by_person = GROUP recent_access_log BY ByWho;
active_users = FOREACH access_by_person GENERATE group AS personID;

-- Join user info with active users
active_user_list = JOIN pages BY personID LEFT OUTER, active_users BY personID;

-- Filter disconnected users (users without recent access log entries)
disconnected_users = FILTER active_user_list BY active_users::personID IS NULL;

result = FOREACH disconnected_users GENERATE pages::personID AS personID, pages::Name AS Name;

STORE result INTO '/output' USING PigStorage(',');
