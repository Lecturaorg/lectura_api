UPDATE TEXTS t
SET author_id = new_author_id.author_id
from (
SELECT DISTINCT 
	t.text_id
	,t.text_author
	,a.author_name
	,a.author_id
FROM texts t
JOIN authors a ON split_part(a.author_name, ', ', 1) = split_part(t.text_author, ', ', 1)
where author_name != 'Plato, Plato Comicus, Πλάτων Κωμικός'--t.text_id = 30
) new_author_id 
where new_author_id.text_id = t.text_id




