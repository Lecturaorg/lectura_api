--By Nationality
select nationality, count(*) cnt_by_country
FROM (
  SELECT UNNEST(string_to_array(author_nationality, ',')) AS nationality
  from texts t
left join authors a on a.author_id::varchar = t.author_id::varchar
) AS subquery
group by nationality
order by cnt_by_country desc

--By century
select (coalesce(author_birth_year,author_death_year)/100)+1 century
,count(*) cnt_by_century
from texts t
left join authors a on a.author_id::varchar = t.author_id::varchar
group by (coalesce(author_birth_year,author_death_year)/100)+1
order by century desc

--Authors missing birth & death year
select distinct a.author_name,a.author_floruit
from texts t
left join authors a on a.author_id::varchar = t.author_id::varchar
where coalesce(author_birth_year,author_death_year)/100 is null and a.author_id is not null
order by author_floruit

---Texts missing author
select distinct t.*
from texts t
left join authors a on a.author_id::varchar = t.author_id::varchar
where t.author_id is null


