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
    ,text_ids
    ,text_titles
    ,text_publications
from authors a
left join (
	select author_id
		,GROUP_CONCAT(text_id SEPARATOR '|') as text_ids
		,GROUP_CONCAT(text_title SEPARATOR '|') as text_titles
		,GROUP_CONCAT(text_original_publication_year SEPARATOR '|') as text_publications
    from texts
    where author_id is not null
    group by author_id
 ) t on t.author_id = a.author_id