select
t.text_id
,text_title
,text_author
,author_id author_id
,text_type
,text_genre
,text_language
,text_original_publication_date
,text_original_publication_year
,text_original_publication_month
,text_original_publication_day
,text_original_publication_publisher
,text_original_publication_publisher_loc
,text_original_publication_type
,text_original_publication_length
,text_original_publication_length_type
,text_writing_start
,text_writing_end
,text_title || 
	case
		when text_original_publication_year is null then ' - ' 
		when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') - '
		else ' (' || text_original_publication_year || ' AD' || ') - '
	end
	|| coalesce(text_author,'Unknown')
	as label
,text_added_date
from texts t

union all

select
t.text_id
,coalesce(title,"bookLabel") as text_title
,author as text_author
,null as author_id
,null as text_type
,null as text_genre
,languagelabel text_language
,date as text_original_publication_date
,publ_year as text_original_publication_year
,null as text_original_publication_month
,null as text_original_publication_day
,null as text_original_publication_publisher
,null as text_original_publication_publisher_loc
,null as text_original_publication_type
,null as text_original_publication_length
,null as text_original_publication_length_type
,null as text_writing_start
,null as text_writing_end
,coalesce(title,"bookLabel") || 
	case
		when publ_year is null then ' - ' 
		when publ_year <0 then ' (' || abs(publ_year) || ' BC' || ') - '
		else ' (' || publ_year || ' AD' || ') - '
	end
	|| coalesce(author,'Unknown')
	as label
,null as text_added_date
from wikitexts_reform t
LEFT JOIN (
	SELECT DISTINCT text_title,"bookLabel",book 
	FROM texts t2
	join (select distinct "bookLabel", book from wikitexts_reform) t
	ON LOWER(t2."text_title") LIKE '%' || LOWER(t."bookLabel") || '%' 
) t2 ON t."bookLabel" LIKE '%' || t2.text_title || '%'--where 
