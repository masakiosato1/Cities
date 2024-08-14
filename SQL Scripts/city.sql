--drop rows where source = 'point' where military = false where ranking = 3, 4, 5 where incorporated = false
--drop source column
--drop id
--drop military
--drop ranking
--drop incorporated
--drop timezone

drop table if exists city;

select 
	city, 
	state_id,
	county_fips,
	lat,
	lng,
	population,
	density,
	zips
into table city
from city_import
where 
	source = 'shape' and 
	military = false and 
	incorporated = true and
	population > 24999