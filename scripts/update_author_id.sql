UPDATE TEXTS t
join authors a on SUBSTRING_INDEX(a.author_name, ", ",1) = SUBSTRING_INDEX(t.text_author, ", ",1)
SET t.AUTHOR_ID = a.author_id
where T.TEXT_ID in (
select distinct text_id, a.author_id
from texts t
join authors a on SUBSTRING_INDEX(a.author_name, ", ",1) = SUBSTRING_INDEX(t.text_author, ", ",1)
)
