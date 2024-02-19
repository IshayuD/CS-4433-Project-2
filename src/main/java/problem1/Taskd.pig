friends = LOAD 'friends.csv' USING PigStorage(',') as (FriendRel: int, PersonID: int, MyFriend: int, DateOfFriendship: DateTime, Desc: chararray);

pages = LOAD 'pages.csv' USING PigStorage(',') as (ID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

joined = JOIN pages BY ID LEFT OUTER, friends BY MyFriend;
cleaned = FOREACH joined GENERATE Name, (MyFriend IS NOT NULL ? 1 : 0) as IsFriend;
grouped = GROUP cleaned BY Name;
connected = FOREACH grouped GENERATE group AS Name, SUM(cleaned.IsFriend) AS NumberOfFriends;
ord = ORDER connected BY NumberOfFriends;


STORE ord INTO 'taskdresults8' USING PigStorage(',');
