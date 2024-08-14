drop table if exists state_mapping;

select distinct
	state_id,
	state_name
into table state_mapping
from city_import