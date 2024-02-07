---Labels where there is a language match
DROP TABLE IF EXISTS temp_wiki_labels;
select distinct l.text,l."textLabel", lan."languageLabel"
INTO temp_wiki_labels
from wikilabels l
left join wikilanguages lan on lan."languageCode" = l.lang
left join texts t on t.text_q = l.text
left join authors a on a.author_q = t.text_author_q
where coalesce(SPLIT_PART(text_language, ', ', 1), SPLIT_PART(author_name_language, ', ', 1)) != lan."languageLabel";

UPDATE TEXTS texts
SET text_title = orig."textLabel"
from (select * from temp_wiki_labels) orig
where texts.text_q = orig.text and texts.text_title is null;
DROP TABLE IF EXISTS temp_wiki_labels;