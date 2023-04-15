drop table if exists authors_wiki;
select
	authorQ
	,max(author) author
	,max(concat(birthYear,'-',replace(birthMonth, '-',''),'-', replace(birthDay,'-',''))) 
		birthDate
	,max(birthYear) birthYear
	,max(replace(birthMonth, '-',''))::int birthMonth
	,max(replace(birthDay,'-',''))::int birthDay
	,max(concat(replace(deathYear,'-',''), '-',deathMonth, '-', deathDay)) deathDate
	,max(replace(deathYear,'-',''))::int deathYear
	,max(deathMonth)::int deathMonth
	,max(deathDay)::int deathDay
	,string_agg(distinct occupation, ', ') occupations
	,max(country) country
	,max(birthplace) birthplace
	,max(deathplace) deathplace
	,max(floruit) floruit
into authors_wiki
from (
SELECT distinct author as authorQ
	, "authorLabel" as author
	, "birthdateLabel" as birthDate
	,case
		when left("birthdateLabel",1) = '-' then left("birthdateLabel",5)::int
		else left("birthdateLabel",4)::int 
	end as birthYear
	,case
		when left("birthdateLabel",1) = '-' then right(left("birthdateLabel",8),2) 
		else right(left("birthdateLabel",7),2) 
	end as birthMonth
	,case
		when left("birthdateLabel",1) = '-' then right(left("birthdateLabel",11),2)  
		else right(left("birthdateLabel",10),2) 
	end as birthDay
	, "deathdateLabel" as deathDate
	,case
		when left("deathdateLabel",4) like '%http%' then null
		when left("deathdateLabel",1) = '-' then left("deathdateLabel",5)
		else left("deathdateLabel",4) 
	end as deathYear
	,case
		when left("deathdateLabel",4) like '%http%' then null
		when left("deathdateLabel",1) = '-' then right(left("deathdateLabel",8),2) 
		else right(left("deathdateLabel",7),2) 
	end as deathMonth
	,case
		when left("deathdateLabel",4) like '%http%' then null
		when left("deathdateLabel",1) = '-' then right(left("deathdateLabel",11),2) 
		else right(left("deathdateLabel",10),2) 
	end as deathDay
	, "occupationsLabel" as occupation
	, country as country
	, "birthplaceLabel" as birthplace
	, "deathplaceLabel" as deathplace
	, "floruitLabel" as floruit
FROM public."WIKI_AUTHORS"
	) a
group by authorQ
order by deathyear;