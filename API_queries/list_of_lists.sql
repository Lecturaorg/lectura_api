select 
    list_id
    ,list_name
    ,list_description
    ,list_type
    ,list_created
    ,list_modified
    ,l.user_id
    ,u.user_name
from user_lists l
join users u on u.user_id = l.user_id