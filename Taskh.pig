friends = LOAD 'friends.csv' USING PigStorage(',') as (FriendRel: int, PersonID: int, MyFriend: int, DateOfFriendship: DateTime, Desc: chararray);

pages = LOAD 'pages.csv' USING PigStorage(',') as (PID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

j = FOREACH (JOIN pages BY PID, friends by MyFriend) GENERATE FriendRel, PID, Name;

-- Count the number of friends for each ID
groupA = GROUP j BY (PID, Name);
c = FOREACH groupA GENERATE group.PID, group.Name, COUNT(j) as count;

-- Get the average number of friends
groupB = GROUP c ALL;
average = FOREACH groupB GENERATE AVG(c.count) as avg;

-- Find all above average
fin = FILTER c BY count > average.avg;

STORE fin INTO 'taskhresults' USING PigStorage(',');
