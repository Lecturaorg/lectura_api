BEGIN;
DROP VIEW IF EXISTS AUTHORS_INCL_LABEL;
CREATE VIEW AUTHORS_INCL_LABEL AS
select 
    a.author_id
/*    ,author_name
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
    ,author_floruit*/
	,'' as texts
	,CONCAT(
    SPLIT_PART(author_name, ', ', 1),
    COALESCE(
        CASE
        WHEN author_birth_year IS NULL AND author_death_year IS NULL AND author_floruit IS NULL THEN ''
        WHEN author_birth_year IS NULL AND author_death_year IS NULL 
			THEN CONCAT(' (fl.', abs(left(author_floruit,4)), ' BC)')
        WHEN author_birth_year IS NULL THEN CONCAT(' (d.', 
                CASE 
                    WHEN author_death_year<0 THEN CONCAT(ABS(author_death_year)::VARCHAR, ' BC')
                    ELSE CONCAT(author_death_year::VARCHAR, ' AD')
				END
            ,
            ')')
        WHEN author_death_year IS NULL THEN CONCAT(' (b.', 
            CASE
				WHEN author_birth_year<0 THEN CONCAT(ABS(author_birth_year)::VARCHAR, ' BC')
            	ELSE concat(author_birth_year::VARCHAR, ' AD')
			END
            ,
            ')')
        ELSE CONCAT(' (', ABS(author_birth_year), '-',
            CASE 
				WHEN author_death_year<0 THEN CONCAT(ABS(author_death_year)::VARCHAR, ' BC')
            	ELSE CONCAT(author_death_year::VARCHAR, ' AD')
			END
            ,
            ')')
        END,
        ''
    )
    ) AS label
	,author_added_date
from authors a
where author_birth_year is not null or author_floruit is not null
/*left join (select distinct 
		   author_id
		   , CASE 
		   		WHEN author_death_year<0 then abs(author_death_year)::varchar|| ' BC'
		   		when author_death_year>0 then author_death_year::varchar || ' AD'
		   	else author_death_year::varchar
		   end as author_death_year_str
		   , CASE 
		   		WHEN author_birth_year<0 then abs(author_birth_year)::varchar|| ' BC'
		   		when author_birth_year>0 then author_birth_year::varchar || ' AD'
		   	else author_birth_year::varchar
		   end as author_birth_year_str
		   from authors) a2 using(author_id)*/;--on a2.author_id = a.author_id;
COMMIT;


