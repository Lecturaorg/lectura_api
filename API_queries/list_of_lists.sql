select 
    -1*list_id as list_id
    ,list_name
    ,list_description
    ,list_type
    ,null as list_created
    ,null as list_modified
    ,null as user_id
    ,null as user_name
    ,list_url
    ,null as list_deleted
    ,'official' as tab
from official_lists l

union all

select 
    list_id
    ,list_name
    ,list_description
    ,list_type
    ,list_created
    ,list_modified
    ,l.user_id
    ,u.user_name
    ,null as list_url
    ,list_deleted
    ,'personal' as tab
from user_lists l
join users u on u.user_id = l.user_id