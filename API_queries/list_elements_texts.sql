SET statement_timeout = 60000;
SELECT 
    element_id
    ,list_id
    ,l.value
    ,text_title || 
        case
            when text_original_publication_year is null then ' - ' 
            when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') - '
            else ' (' || text_original_publication_year || ' AD' || ') - '
        end
        || coalesce(text_author,'Unknown')
        as label
    ,text_title
    ,text_author
    ,text_original_publication_year
    ,case when c.text_id is not null then true else false end as checks
    ,case when w.text_id is not null then true else false end as watch
FROM USER_LISTS_ELEMENTS L
join texts t on t.text_id = l.value
left join (select distinct text_id from checks where user_id = [$user_id]) c on c.text_id = L.element_id
left join (select distinct text_id from watch where user_id = [$user_id]) w on w.text_id = L.element_id
--join authors a on a.author_id::varchar(255) = t.author_id
WHERE LIST_ID = [@list_id]