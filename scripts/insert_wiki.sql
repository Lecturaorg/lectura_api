--Reformat wikidata
drop table if exists wikiauthors_reform;
select distinct 
	--row_number() over (order by a.author)+max(author_id) author_id
	a.author
	,max(an."authorLabel") "authorLabel"
	,case
		when left(min(a.birthdate),4) = 'http' then null
		else min(a.birthdate)
		end as birthdate
	,case
		when left(min("birthdate"),4) = 'http' then null
		when left(min("birthdate"),1) = '-' then left(min("birthdate"),5)::int
		else left(min("birthdate"),4)::int 
	end as birthYear
	,case
		when left(min("birthdate"),4) = 'http' then null
		when left(min("birthdate"),1) = '-' then right(left(min("birthdate"),8),2) 
		else right(left(min("birthdate"),7),2) 
	end as birthMonth
	,case
		when left(min("birthdate"),4) = 'http' then null
		when left(min("birthdate"),1) = '-' then right(left(min("birthdate"),11),2)  
		else right(left(min("birthdate"),10),2) 
	end as birthDay
	,case 
		when left(min(a.deathdate),4)='http' then null
		else min(a.deathdate)
		end as deathdate
	,case
		when left(min("deathdate"),4) = 'http' then null
		when left(min("deathdate"),1) = '-' then left(min("deathdate"),5)::int
		else left(min("deathdate"),4)::int 
	end as deathYear
	,case
		when left(min("deathdate"),4) = 'http' then null
		when left(min("deathdate"),1) = '-' then right(left(min("deathdate"),8),2) 
		else right(left(min("deathdate"),7),2) 
	end as deathMonth
	,case
		when left(min("deathdate"),4) = 'http' then null
		when left(min("deathdate"),1) = '-' then right(left(min("deathdate"),11),2)  
		else right(left(min("deathdate"),10),2) 
	end as deathDay
	,min(a.floruit) floruit
	,max(bp."birthplaceLabel") birthplace
	,max(c."countryLabel") as birthcountry--bp.birthplacecountry --Replace with countryTable
	,string_agg(distinct p."professionLabel",', ') occupations
	,string_agg(distinct a.country, ', ') country
	,max(dp."deathplaceLabel") deathplace
	,max(c2."countryLabel") as deathcountry
	,string_agg(distinct l."languageLabel",', ') languages
into wikiauthors_reform
from wikiauthors a 
left join (select distinct author, "authorLabel" from wikiauthorname) an on an.author = a.author
left join (select distinct birthplace, "birthplaceLabel", birthplacecountry from wikibirthplace)
	bp on bp.birthplace = a.birthplace
left join (select distinct deathplace, "deathplaceLabel", deathplacecountry from wikideathplace) dp on dp.deathplace = a.deathplace
left join wikiprofessions p on p.profession = a.occupations
left join wikicountries c on c.country = bp.birthplacecountry
left join wikicountries c2 on c2.country = dp.deathplacecountry
left join wikilanguages l on l.language = a.languages
--left join (select max(author_id) author_id from authors) author_id on 1=1
where a.country is not null 
	--and not ("birthdate" is null and floruit is null)
group by a.author;

drop table if exists wikitexts_reform;
select *
into wikitexts_reform
from (
select distinct
	--row_number() over (order by t.book)+max(text_id) as text_id
	t.book
	,t."bookLabel"
	,string_agg(distinct t.author, ', ') as authorQ
	,string_agg(distinct an."authorLabel",', ') author
	,string_agg(distinct t.title, '|') title
	,string_agg(distinct l."languageLabel",', ') languageLabel
	,string_agg(distinct c2."countryLabel",', ') originCountry
	,max(t.date) date
	,max(case
		when left("date",4) = 'http' then coalesce(deathyear, null)
		when "date" like '[A-Za-z]%' then coalesce(deathyear, null)
		when "date" like '%2SPA%' then coalesce(deathyear, null)
		when left("date",1) = '-' then left("date",5)::int
		else left("date",4)::int 
	end) as publ_year
	,max(case
		when left("date",4) = 'http' then null
		when "date" like '[A-Za-z]%' then null
		when "date" like '%2SPA%' then null
		when left("date",1) = '-' then right(left("date",8),2)::int
		else right(left("date",7),2)::int 
	end) as publ_month
	,max(case
		when left("date",4) = 'http' then null
		when "date" like '[A-Za-z]%' then null
		when "date" like '%2SPA%' then null
		when left("date",1) = '-' then right(left("date",11),2)::int
		else right(left("date",10),2)::int 
	end) as publ_day
	,string_agg(distinct t.country, ', ') as authorCountry
	,string_agg(distinct typ."typeLabel", ', ') as textType
from wikitexts t
left join wikiauthorname an on an.author = t.author
left join (select min("deathyear") deathyear, min("floruit") floruit, author from wikiauthors_reform group by author)
		death on death.author = t.author
left join wikilanguages l on l.language = t.language
--left join wikicountries c on c.country = t.country
left join wikicountries c2 on c2.country = t.origincountry
left join wikitypes typ on typ.type = t.type
--left join (select max(text_id) text_id from texts) text_id on 1=1
where t.type in (
'http://www.wikidata.org/entity/Q7725634'
,'http://www.wikidata.org/entity/Q47461344'
,'http://www.wikidata.org/entity/Q571'
,'http://www.wikidata.org/entity/Q5185279'
,'http://www.wikidata.org/entity/Q49084'
,'http://www.wikidata.org/entity/Q8261'
,'http://www.wikidata.org/entity/Q20540385'
,'http://www.wikidata.org/entity/Q25379'
,'http://www.wikidata.org/entity/Q386724'
,'http://www.wikidata.org/entity/Q23622'
,'http://www.wikidata.org/entity/Q149537'
,'http://www.wikidata.org/entity/Q36279'
,'http://www.wikidata.org/entity/Q699'
,'http://www.wikidata.org/entity/Q4184'
,'http://www.wikidata.org/entity/Q112983'
,'http://www.wikidata.org/entity/Q25839930'
,'http://www.wikidata.org/entity/Q17518461'
,'http://www.wikidata.org/entity/Q482'
,'http://www.wikidata.org/entity/Q856713'
,'http://www.wikidata.org/entity/Q1318295'
,'http://www.wikidata.org/entity/Q59126'
,'http://www.wikidata.org/entity/Q25372'
,'http://www.wikidata.org/entity/Q37484'
,'http://www.wikidata.org/entity/Q58854'
,'http://www.wikidata.org/entity/Q241996'
,'http://www.wikidata.org/entity/Q8242'
,'http://www.wikidata.org/entity/Q40831'
,'http://www.wikidata.org/entity/Q182659'
,'http://www.wikidata.org/entity/Q24723'
,'http://www.wikidata.org/entity/Q80930'
,'http://www.wikidata.org/entity/Q182357'
,'http://www.wikidata.org/entity/Q44342'
,'http://www.wikidata.org/entity/Q128758'
,'http://www.wikidata.org/entity/Q114375'
,'http://www.wikidata.org/entity/Q208628'
,'http://www.wikidata.org/entity/Q1640824'
,'http://www.wikidata.org/entity/Q179461'
)
group by 
	t.book
	,t."bookLabel"
	--,t.author
	--,l."languageLabel"
	--,t.date
) agg
where (publ_year <=2000 or publ_year is null) and "bookLabel" is not null;
--Update texts with updated wiki data
update texts t
set text_title = t2.text_title
	,text_author = t2.text_author,
	text_type = t2.text_type, text_language = t2.text_language,
	text_original_publication_date = t2.text_original_publication_date,
	text_original_publication_year = t2.text_original_publication_year,
	text_original_publication_month = t2.text_original_publication_month,
	text_original_publication_day = t2.text_original_publication_day,
	text_author_q = t2.text_author_q
from (
select 
	coalesce(CASE 
			 	WHEN title like '%http://www.wikidata.org/.well-known%' then "bookLabel"
			 	ELSE "bookLabel" END,
			title)
			 as text_title
	,author as text_author
	,null as author_id
	,texttype as text_type
	,null as text_genre
	,languagelabel as text_language
	,case 
		when date like '%http://www.wikidata.org/.well-known%' then null 
		else date 
	end as text_original_publication_date
	,publ_year as text_original_publication_year
	,publ_month as text_original_publication_month
	,publ_day as text_original_publication_day
	,null as text_original_publication_publisher
	,null as text_original_publication_publisher_loc
	,null as text_original_publication_type
	,null as text_original_publication_length
	,null as text_original_publication_length_type
	,null as text_writing_start
	,null as text_writing_end
	,book as text_q
	,authorq as text_author_q
from wikitexts_reform) t2
where t.text_q = t2.text_q and t.text_q is not null;

--Update existing authors with updated data from wikidata
UPDATE AUTHORS a
set author_name = a2.author_name, author_nationality = a2.author_nationality
	,author_positions = a2.author_positions
	,author_name_language = a2.author_name_language
	,author_birth_date = a2.author_birth_date
	,author_birth_city = a2.author_birth_city, author_birth_country = a2.author_birth_country,
	author_death_date = a2.author_death_date, author_death_city = a2.author_death_city,
	author_death_country = a2.author_death_country, author_death_year = a2.author_death_year,
	author_death_month = a2.author_death_month, author_death_day = a2.author_death_day,
	author_floruit = a2.author_floruit
from (
select 
coalesce("authorLabel",author_name) as author_name
,coalesce(country,author_nationality) as author_nationality
,coalesce(occupations, author_positions) as author_positions
,coalesce(languages, author_name_language) as author_name_language
,coalesce(birthdate, author_birth_date) as author_birth_date
,coalesce(birthplace, author_birth_city) as author_birth_city
,coalesce(birthcountry, author_birth_country) as author_birth_country
,coalesce(birthyear,author_birth_year) as author_birth_year
,coalesce(birthmonth::int, author_birth_month) as author_birth_month
,coalesce(birthday::int,author_birth_day) as author_birth_day
,coalesce(deathdate,author_death_date) as author_death_date
,coalesce(deathplace,author_death_city) as author_death_city
,coalesce(deathcountry,author_death_country) as author_death_country
,coalesce(deathyear,author_death_year) as author_death_year
,coalesce(deathmonth::int,author_death_month) as author_death_month
,coalesce(deathday::int,author_death_day) as author_death_day
,case 
	when coalesce(floruit,author_floruit) like '%http://www.wikidata.org/.well-known/genid%' then null 
	else coalesce(floruit,author_floruit) 
end as author_floruit
,author as author_q
from wikiauthors_reform a
left join authors a2 on a2.author_q = a.author
where coalesce("authorLabel",author_name) is not null
) a2
where a2.author_q = a.author_q and a.author_q is not null;

--Insert authors that do not exist in the author table from wikireformat
INSERT INTO AUTHORS (author_name,author_positions,author_name_language
					 ,author_birth_date,author_birth_city
					 ,author_birth_country
					 ,author_birth_year,author_birth_month,author_birth_day
					 ,author_death_date,author_death_city,author_death_country
					 ,author_death_year,author_death_month
					 ,author_death_day,author_nationality
					 ,author_floruit,author_q)
select 
"authorLabel" as author_name
,occupations as author_positions
,languages as author_name_language
,birthdate as author_birth_date
,birthplace as author_birth_city
,birthcountry as author_birth_country
,birthyear as author_birth_year
,birthmonth::int as author_birth_month
,birthday::int as author_birth_day
,deathdate as author_death_date
,deathplace as author_death_city
,deathcountry as author_death_country
,deathyear as author_death_year
,deathmonth::int as author_death_month
,deathday::int as author_death_day
,country as author_nationality
,floruit as author_floruit
,author as author_q
from wikiauthors_reform a
left join authors a2 on a2.author_q = a.author
where a2.author_q is null;

--Insert texts that do not exist in the texts table from wikidata
INSERT INTO TEXTS 
		(text_title,text_author,author_id,text_type,text_genre,text_language
		 ,text_original_publication_date,text_original_publication_year
		 ,text_original_publication_month,text_original_publication_day
		,text_q,text_author_q)
select 
	coalesce(CASE 
			 	WHEN title like '%http://www.wikidata.org/.well-known%' then "bookLabel"
			 	ELSE title END,
			"bookLabel")
			 as text_title
	,author as text_author
	,null as author_id
	,texttype as text_type
	,null as text_genre
	,languagelabel as text_language
	,case 
		when date like '%http://www.wikidata.org/.well-known%' then null 
		else date 
	end as text_original_publication_date
	,publ_year as text_original_publication_year
	,publ_month as text_original_publication_month
	,publ_day as text_original_publication_day
	,book as text_q
	,authorq as text_author_q
from wikitexts_reform t
left join texts t2 on t2.text_q = t.book
where t2.text_q is null;

--Update author_id in texts
UPDATE texts t
SET author_id = a.author_id
FROM (SELECT distinct t.text_q, string_agg(a.author_id::varchar(255), ', ') author_id
	, string_agg(a.author_q, ', ') author_q
FROM (select text_q
	  , unnest(string_to_array(text_author_q,', ')) as text_author_q
	 from texts
	 ) t
LEFT JOIN authors a ON a.author_q = t.text_author_q
group by t.text_q) a
WHERE a.author_q = t.text_author_q and t.author_id is null and t.text_author_q is not null;

--Find duplicates and label them
	--Author duplicates
/*UPDATE authors a
set author_duplicate = 1
FROM
(
select *
from (
select distinct 
	author_id
	,concat(split_part(author_name,',',1),author_birth_year) m_pattern
	,row_number() over (partition by split_part(author_name,', ',1), author_birth_year order by author_q) rn
from authors a
where author_name is not null and author_birth_Year is not null
	) agg where rn!=1
) dupl
where a.author_duplicate is null and a.author_id = dupl.author_id;*/

	--Text duplicates
/*select distinct concat(t.text_title, split_part(t.text_author,', ',1)) m_pattern
from texts t
join (
select distinct
	text_id
	,text_q
	,concat(text_title, split_part(text_author,', ',1)) m_pattern
	,row_number() over (partition by text_title, split_part(text_author,', ',1) order by text_q desc) rn
from texts
	) t2 on m_pattern = concat(t.text_title, split_part(t.text_author,', ',1))
where rn>1*/

--Add duplicates to authors_duplicate
/*INSERT INTO authors_duplicate
select *
from authors
where author_duplicate is not null;
--Remove duplicates from authors
DELETE FROM authors
where author_duplicate is not null;
--Add texts that is on duplicate authors to texts_duplicate
INSERT INTO texts_duplicate
SELECT
t.*
from texts t
join authors_duplicate a on a.author_id::varchar(255) = t.author_id;
--Remove texts that is on duplicate authors from texts
delete from texts t
USING authors_duplicate a
where t.author_id::varchar(255) = a.author_id::varchar(255);
*/
--ALTER TABLE TEXTS ALTER COLUMN author_id TYPE varchar(255)
/*--Refresh text_id
CREATE SEQUENCE temp_text_id_seq START 1;
--SELECT setval('temp_text_id_seq', (SELECT count(text_id) FROM texts));
UPDATE texts SET text_id = nextval('temp_text_id_seq');
DROP SEQUENCE texts_text_id_seq;
ALTER SEQUENCE temp_text_id_seq RENAME TO texts_text_id_seq;
SELECT setval('texts_text_id_seq', (SELECT COUNT(*) FROM texts));
SELECT last_value FROM texts_text_id_seq;
--Refresh author_id
CREATE SEQUENCE temp_author_id_seq START 1;
--SELECT setval('temp_author_id_seq', (SELECT count(author_id) FROM authors));
UPDATE authors SET author_id = nextval('temp_author_id_seq');
DROP SEQUENCE author_author_id_seq;
ALTER SEQUENCE temp_author_id_seq RENAME TO authors_author_id_seq;
SELECT setval('authors_author_id_seq', (SELECT COUNT(*) FROM authors));
SELECT last_value FROM authors_author_id_seq;
*/

/*select distinct text_id from texts order by text_id
where author_id is not null
select a.author_id, a.author_name,a.author_birth_year, count(*) cnt
from authors a
join texts t on t.text_author_q = a.author_q
join wikiauthors_reform a2 on a2.author = a.author_q
group by a.author_id, a.author_name,a.author_birth_year
order by cnt desc
select * from getTexts(55000)

SELECT a.*
FROM authors a
join wikiauthors_reform wa on wa.author = a.author_q
WHERE DATE_TRUNC('day', author_added_date) = DATE_TRUNC('day', '2023-04-14'::DATE)
ORDER BY author_added_date desc;
select * from wikiauthors
where author = 'http://www.wikidata.org/entity/Q4497214'

select max(author_id) max_id, count(*) cnt from authors;
select max(text_id) max_id, count(*) cnt from texts;
select * from authors
where author_name like '%Jonas %'
select * from wikiauthors where "countryQ" = 'Q389688'
select * from texts
where text_title = 'To the Rainbow'

select count(*) cnt, author_birth_country
from authors
group by author_birth_country
order by cnt desc


where author_birth_country is null and author_birth_city is not null

select * from wikiprofessions
where "professionLabel" like '%monarch%'

select t.text_title,t.text_q,a.author_name_language, t.text_language
from texts t
left join authors a on a.author_q = t.text_author_q
where not (a.author_name_language like concat('%',t.text_language,'%') or t.text_language is null)
and a.author_id = '129200'
 select * from wikiauthors_reform
 where country = 'Kingdom of England'--author = 'https://www.wikidata.org/wiki/Q692'

where author_birth_date is null and author_floruit != ''
order by author_floruit


*/
