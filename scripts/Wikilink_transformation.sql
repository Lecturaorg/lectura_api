---WikiSource
drop table if exists texts_sources;
select 
	l.url
	,l.item
	,t.text_title
	,t.text_author
	,lan."languageLabel" lang
	,lan."languageCode" lang_code
INTO texts_sources
from wikilinks l
join texts t on t.text_q = l.item
left join wikilanguages lan on lan."languageCode" = SUBSTRING(l.url FROM 'https://([a-z]+)')
where l.url ILIKE '%wikisource%';
--Wikipedia urls - Author
drop table if exists author_wikipedia_urls
select 
	l.url
	,l.item
	,a.author_name
	,a.author_id
	,lan."languageLabel" lang
	,lan."languageCode" lang_code
into author_wikipedia_urls
from wikilinks l
join authors a on a.author_q = l.item
left join wikilanguages lan on lan."languageCode" = SUBSTRING(l.url FROM 'https://([a-z]+)')
--select author_wikipedia from authors

