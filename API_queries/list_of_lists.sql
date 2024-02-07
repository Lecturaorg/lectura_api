select 
    -1*l.list_id as list_id
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
    ,coalesce(lik.likes,0) likes
    ,coalesce(dis.dislikes,0) dislikes
    ,coalesce(watch.watchlists,0) watchlists
from official_lists l
left join (select count(*) likes, list_id from user_lists_likes group by list_id) lik on lik.list_id = l.list_id*-1
left join (select count(*) dislikes, list_id from user_lists_dislikes group by list_id) dis on dis.list_id = l.list_id*-1
left join (select count(*) watchlists, list_id from user_lists_watchlists group by list_id) watch on watch.list_id = l.list_id*-1

union all

select 
    l.list_id
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
    ,coalesce(lik.likes,0) likes
    ,coalesce(dis.dislikes,0) dislikes
    ,coalesce(watch.watchlists,0) watchlists
from user_lists l
join users u on u.user_id = l.user_id
left join (select count(*) likes, list_id from user_lists_likes group by list_id) lik on lik.list_id = l.list_id
left join (select count(*) dislikes, list_id from user_lists_dislikes group by list_id) dis on dis.list_id = l.list_id
left join (select count(*) watchlists, list_id from user_lists_watchlists group by list_id) watch on watch.list_id = l.list_id
