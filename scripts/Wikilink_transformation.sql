---WikiSource
drop table if exists texts_sources;
select 
	l.url
	,l.item
	,t.text_title
	,t.text_author
	,lan."languageLabel"
	,lan."languageCode"
INTO texts_sources
from wikilinks l
join texts t on t.text_q = l.item
left join wikilanguages lan on lan."languageCode" = SUBSTRING(l.url FROM 'https://([a-z]+)')
where l.url ILIKE '%wikisource%';


