SELECT 
    element_id
    ,list_id
    ,l.value
FROM USER_LISTS_ELEMENTS L
join texts a on a.text_id = l.value
WHERE LIST_ID = [@list_id]