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
	and not ("birthdate" is null and floruit is null)
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


