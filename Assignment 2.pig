-- CSV is loaded into the orders variable, columns seperated by ",". 
-- so the columns can be stored seperatly in the names given in the AS (...)
-- chararray is the type of value that is used, which is a string 
orders = LOAD '/user/maria_dev/diplomacy/orders.csv'
         USING PigStorage(',')
         AS
         (game_id:chararray,
         unit_id:chararray,
         unit_order:chararray,
         location:chararray,
         target:chararray,
         target_dest:chararray,
         success:chararray,
         reason:chararray,
         turn_num:chararray);

-- This is a filter that filters the rows orders, and returns the rows that have a target equal to '"Holland"'  
filter_by_location = FILTER orders BY target == '"Holland"';

-- Here we group the location with the target ("Holland"), which results in (<location>, "Holland")
listGroup = GROUP filter_by_location BY (location, target);

-- Now we would need to know how many times '"Holland"' was the "target" of the "location".
-- So we loop over the group list and generate new rows with the data of the listGroup variable
-- And the count of the filtered locations
countList = FOREACH listGroup GENERATE group, COUNT(filter_by_location);

-- We order the countList by the first column (Location) by ascending (asc) order (A to Z)
sortList = ORDER countList by $0 ASC;

-- Lastly we show the results in the output
DUMP sortList;