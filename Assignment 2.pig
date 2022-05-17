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

filter_by_location = FILTER orders BY target == '"Holland"';             
listGroup = GROUP filter_by_location BY (location, target);
countList = FOREACH listGroup GENERATE group, COUNT(filter_by_location);
sortList = ORDER countList by $0 ASC;
DUMP sortList;