BEGIN;
DROP VIEW IF EXISTS TEXTS_INCL_LABEL;
CREATE VIEW TEXTS_INCL_LABEL AS
select
t.text_id
,author_id
,text_title
,text_author
/*,author_id author_id
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
,text_writing_end*/
,text_title || 
	case
		when text_original_publication_year is null then ' - ' 
		when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') - '
		else ' (' || text_original_publication_year || ' AD' || ') - '
	end
	|| coalesce(text_author,'Unknown')
	as label
,text_added_date
from texts t;
COMMIT;
