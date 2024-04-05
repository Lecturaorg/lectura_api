drop table if exists author_bookcount;
select * 
into author_bookcount
from (
select a.author_id, count(*) book_cnt
from authors a
join texts t on t.author_id::varchar = a.author_id::varchar
group by a.author_id) ag where book_cnt >0