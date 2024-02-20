
access = LOAD 'access_logs.csv' USING PigStorage(',') as (AccessID: int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime: DateTime);

agroup = GROUP access BY WhatPage;


page_visits = FOREACH agroup GENERATE group as grp, COUNT(access.WhatPage) as count;
ord = ORDER page_visits BY count DESC; 
lord = LIMIT ord 10;


STORE lord INTO 'taskBresults3' USING PigStorage(',');