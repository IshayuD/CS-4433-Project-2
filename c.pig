pages = LOAD 'data_set/pages.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

friends = LOAD 'data_set/friends.csv' USING PigStorage(',') AS (FriendRel:int, PersonID:int, MyFriend:int, DateofFriendship:int, Desc:chararray);

user_with_facebook = JOIN friends BY PersonID, pages BY ID;

-- Group by country and count the number of citizens with a Facebook page
result = FOREACH (GROUP user_with_facebook BY Nationality) {
    facebook_count = COUNT(user_with_facebook);
    GENERATE group AS country, 	facebook_count AS num_citizens_with_facebook;
}

STORE result INTO 'c_output' USING PigStorage(',');
