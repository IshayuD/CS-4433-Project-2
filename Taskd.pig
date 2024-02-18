friends = LOAD 'friends.csv' USING PigStorage(',') as (FriendRel: int, PersonID: int, MyFriend: int, DateOfFriendship: DateTime, Desc: chararray);

pages = LOAD 'pages.csv' USING PigStorage(',') as (ID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

-- Join tables and clean the result
joined = FOREACH (JOIN pages BY ID, friends BY MyFriend) GENERATE FriendRel, ID, Name;

-- count number of friends
grouped = GROUP joined BY (ID, Name);
ord = ORDER (FOREACH grouped GENERATE group.ID, group.Name, COUNT(joined) as numFriends) BY ID ASC;

-- Write out result
STORE ord INTO 'taskdresults6' USING PigStorage(',');
