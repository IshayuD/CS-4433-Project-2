pages = LOAD '/data_set/pages.csv' USING PigStorage(',') AS (personID:int, name:chararray, nationality:chararray, countryCode:int, hobby:chararray);

-- Define nationality
my_nationality = 'Germany';

-- Filter data for users with the same nationality
filtered_data = FILTER pages BY nationality == my_nationality;

result = FOREACH filtered_data GENERATE name, hobby;

DUMP result;
STORE result INTO '/output' USING PigStorage(',');

