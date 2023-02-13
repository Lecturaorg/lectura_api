select
t.text_id
,text_title
,text_author
,author_id
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
,edition_ids
,titles
,publication_years
,additional_authors
,languages
,isbn
,isbn13
from texts t
left join (
	select
		text_id
        ,GROUP_CONCAT(edition_id SEPARATOR '|') as edition_ids
        ,GROUP_CONCAT(edition_title SEPARATOR '|') as titles
        ,GROUP_CONCAT(edition_publication_year SEPARATOR '|') as publication_years
        ,GROUP_CONCAT(edition_editor SEPARATOR '|') as additional_authors
        ,GROUP_CONCAT(edition_language SEPARATOR '|') as languages
        ,GROUP_CONCAT(edition_isbn SEPARATOR '|') as isbn
        ,GROUP_CONCAT(edition_isbn13 SEPARATOR '|') as isbn13
        from editions
        group by text_id
) e on e.text_id = t.text_id