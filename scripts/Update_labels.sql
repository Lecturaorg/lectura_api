--New text_title where it doesnt exist in English
UPDATE texts t
SET text_title = ag.new_text_title
FROM (
    SELECT MAX(COALESCE(l."elementLabel", l2."elementLabel")) AS new_text_title, t.text_q
    FROM texts t
    LEFT JOIN (SELECT * FROM wikilabels WHERE lang = 'en') l ON l.element = t.text_q
    JOIN wikilabels l2 ON l2.element = t.text_q
    JOIN wikilanguages lan ON lan."languageCode" = l2.lang
    WHERE t.text_title IS NULL
    GROUP BY COALESCE(l."elementLabel", l2."elementLabel"), t.text_q
) AS ag
WHERE t.text_q = ag.text_q and t.text_title is null;
--Delete translations
DELETE FROM TEXTS
where text_type = 'translation'

select concat(replace(text_q,'http://www.wikidata.org/entity/',' wd:'),' ') q 
from texts t
left join wikilabels l on l.element = t.text_q
where text_q is not null and l.element is null
select * from texts
where text_id = '49034'

