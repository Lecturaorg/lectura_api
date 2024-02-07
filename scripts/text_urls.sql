select t.text_title,t.text_author,l.url
from wikilinks l
left join texts t on t.text_q = l.item
where url ilike '%wikisource%' and url ilike '%fr.%'
order by text_author desc, text_title desc
