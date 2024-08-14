drop table if exists county_mapping;

select distinct
	county_fips,
	county_name
into table county_mapping
from city_import