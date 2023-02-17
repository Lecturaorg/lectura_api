BEGIN;
DROP VIEW IF EXISTS AUTHORS_INCL_LABEL;
CREATE VIEW AUTHORS_INCL_LABEL AS
select 
    a.author_id
    ,author_name
    ,author_positions
    ,author_name_language
    ,author_birth_date
    ,author_birth_city
    ,author_birth_country
    ,author_birth_coordinates
    ,author_birth_year
    ,author_birth_month
    ,author_birth_day
    ,author_death_date
    ,author_death_city
    ,author_death_country
    ,author_death_coordinates
    ,author_death_year
    ,author_death_month
    ,author_death_day
    ,author_nationality
    ,author_gender
    ,author_floruit
	,'' as texts
	,split_part(author_name,', ',1) || 
	case
		when author_birth_year is null and author_death_year is null and author_floruit is null then ''
		when author_birth_year is null and author_death_year is null then ' (fl.' || author_floruit || ')'
		when author_birth_year is null then ' (d.' || author_death_year_str || ')'
		when author_death_year is null then ' (b.' || author_birth_year_str || ')'
		else ' (' || abs(author_birth_year) || '-' || author_death_year_str || ')'
		end
	as label
from authors a
left join (select distinct 
		   author_id
		   , CASE 
		   		WHEN author_death_year<0 then abs(author_death_year)::varchar(255)|| ' BC'
		   		when author_death_year>0 then author_death_year::varchar(255) || ' AD'
		   	else author_death_year::varchar(255)
		   end as author_death_year_str
		   , CASE 
		   		WHEN author_birth_year<0 then abs(author_birth_year)::varchar(255)|| ' BC'
		   		when author_birth_year>0 then author_birth_year::varchar(255) || ' AD'
		   	else author_birth_year::varchar(255)
		   end as author_birth_year_str
		   from authors) a2 using(author_id)--on a2.author_id = a.author_id;
COMMIT;


