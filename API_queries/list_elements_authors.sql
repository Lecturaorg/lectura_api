SELECT 
    element_id
    ,list_id
    ,l.value
FROM USER_LISTS_ELEMENTS L
join authors a on a.author_id = l.value
WHERE LIST_ID = [@list_id]