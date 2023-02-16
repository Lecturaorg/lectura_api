UPDATE editions e
SET text_id = new_text_id.text_id
from
(select e.edition_id, max(t.text_id) text_id--, e.edition_title, t.text_title, t.text_author
--,row_number() over (partition by e.edition_id order by t.text_title) rn
from editions e
left join texts t on split_part(t.text_title, ', ', 1) = split_part(e.edition_title, ', ', 1)
group by e.edition_id
) new_text_id
where e.edition_id = new_text_id.edition_id
--order by rn desc
