
friends = LOAD 'friends.csv' USING PigStorage(',') as (FriendRel: int, PersonID: int, MyFriend: int, DateOfFriendship: DateTime, Desc: chararray);

pages = LOAD 'pages.csv' USING PigStorage(',') as (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

gr = GROUP friends BY MyFriend;
gr2 = GROUP pages BY PersonID;
noffr = FOREACH gr GENERATE group as grp, COUNT(friends.MyFriend) as count ;
page = FOREACH gr2 GENERATE group as grp2;
j = JOIN noffr BY grp LEFT, page BY grp2 using 'replicated';
fin = FOREACH j GENERATE grp as grp, ( count is NULL ? 0 : count) as count;
ord = ORDER fin BY count ASC;

STORE ord INTO 'taskdresults3' USING PigStorage(',');