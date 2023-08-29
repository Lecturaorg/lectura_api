SET statement_timeout = 60000;
select 
	value
	,type
	,a.author_id::varchar(255) as author_id
	,label
from (
select 
	text_id as value
	,'text' as type
	,text_title || 
	case
		when text_original_publication_year is null then ' - ' 
		when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') - '
		else ' (' || text_original_publication_year || ' AD' || ') - '
	end
	|| coalesce(text_author,'Unknown')
	as label
	,text_author_q
	from texts t
WHERE text_title ilike %(query)s or text_author ilike %(query)s
) t
left join (select distinct author_q, author_id from authors) a on a.author_q = t.text_author_q
UNION ALL
select 
	value
	,type
	,author_id
	,label
from (
    select
    author_id as value
    ,'author' as type
    ,null as author_id
    ,CONCAT(
    SPLIT_PART(author_name, ', ', 1),
    COALESCE(
        CASE
        WHEN author_birth_year IS NULL AND author_death_year IS NULL AND author_floruit IS NULL THEN ''
        WHEN author_birth_year IS NULL AND author_death_year IS NULL THEN CONCAT(' (fl.', left(author_floruit,4), ')')
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
    FROM authors a
	left join AUTHOR_LABELS lab on lab.author = a.author_q
	where author_name ilike %(query)s or lab.author_label ilike %(query)s or author_nationality ilike %(query)s
		or author_positions ilike %(query)s or author_birth_city ilike %(query)s or author_birth_country ilike %(query)s
		or author_name_language ilike %(query)s
) ag
