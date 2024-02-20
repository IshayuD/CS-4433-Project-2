-- Load data files
relationship = LOAD 'friends.csv' USING PigStorage(',') AS (ID:int, PersonID: int, myFriend: int, DateofFriendship: chararray, Description: chararray);
accessLogs = LOAD 'access_logs.csv' USING PigStorage(',') AS (AccessID: int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime: long);


-- Extract the PIDs from both datasets and remove duplicates
distinctRelationships = DISTINCT (FOREACH relationship Generate PersonID, myFriend);
distinctAccesslogs = DISTINCT (FOREACH accessLogs  Generate ByWho, WhatPage);


-- Pair each row with the same values and deleted non shared values (distinctRelationships - distinctAccesslogs)
cogrouped = COGROUP distinctRelationships BY (PersonID, myFriend), distinctAccesslogs BY (ByWho, WhatPage);
no_access = FILTER cogrouped BY IsEmpty(distinctAccesslogs); 
difference = FOREACH no_access GENERATE FLATTEN(distinctRelationships);


-- Group the difference by personID and return just the personIDs
people = FOREACH (GROUP difference BY PersonID) GENERATE group;
ord = ORDER people BY group DESC;

-- Store the result in a file
STORE ord INTO 'TaskF3' USING PigStorage(',');
