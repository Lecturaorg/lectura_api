---Author Details
DROP TABLE IF EXISTS WIKIAUTHOR_DETAILS_TEMP;
SELECT 
	distinct author as author_q
	,string_agg(distinct authordesc, ', ') author_description
	,string_agg(distinct "authorLabel", ', ') author_name
	,string_agg(distinct "akaLabel",', ') author_aka
	,string_agg(distinct "genderLabel", ', ') author_gender
	,max(birthdate) author_birth_date
	,max(case
		when left("birthdate",4) = 'http' then null
		when "birthdate" like '[A-Za-z]%' then null
		when "birthdate" like '%2SPA%' then null
		when left("birthdate",1) = '-' then left("birthdate",5)::int
		else left("birthdate",4)::int 
	end) author_birth_year
	,max(case
		when left("birthdate",4) = 'http' then null
		when "birthdate" like '[A-Za-z]%' then null
		when "birthdate" like '%2SPA%' then null
		when left("birthdate",1) = '-' then right(left("birthdate",8),2)::int
		else right(left("birthdate",7),2)::int 
	end) author_birth_month
	,max(case
		when left("birthdate",4) = 'http' then null
		when "birthdate" like '[A-Za-z]%' then null
		when "birthdate" like '%2SPA%' then null
		when left("birthdate",1) = '-' then right(left("birthdate",11),2)::int
		else right(left("birthdate",10),2)::int 
	end) author_birth_day
	,string_agg(distinct "birthplaceLabel", ', ') author_birth_city
	,string_agg(distinct "birthplacecountryLabel", ', ') author_birth_country
	,max(deathdate) author_death_date
	,max(case
		when left("deathdate",4) = 'http' then null
		when "deathdate" like '[A-Za-z]%' then null
		when "deathdate" like '%2SPA%' then null
		when left("deathdate",1) = '-' then left("deathdate",5)::int
		else left("deathdate",4)::int 
	end) author_death_year
	,max(case
		when left("deathdate",4) = 'http' then null
		when "deathdate" like '[A-Za-z]%' then null
		when "deathdate" like '%2SPA%' then null
		when left("deathdate",1) = '-' then right(left("deathdate",8),2)::int
		else right(left("deathdate",7),2)::int 
	end) author_death_month
	,max(case
		when left("deathdate",4) = 'http' then null
		when "deathdate" like '[A-Za-z]%' then null
		when "deathdate" like '%2SPA%' then null
		when left("deathdate",1) = '-' then right(left("deathdate",11),2)::int
		else right(left("deathdate",10),2)::int 
	end) author_death_day
	,string_agg(distinct "deathplaceLabel", ', ') author_death_city
	,string_agg(distinct "deathplacecountryLabel", ', ') author_death_country
	,max(floruit::varchar(255)) author_floruit
	,string_agg(distinct "occupationsLabel", ', ') author_positions
	,string_agg(distinct "languagesLabel", ', ') author_name_language
	,string_agg(distinct "spouseLabel", ', ') author_spouse
	,string_agg(distinct "imageLabel", ', ') author_image
	,string_agg(distinct "nativenameLabel", ', ') author_name_native
	,string_agg(distinct "citizenshipLabel", ', ') author_citizenship
	,max(distinct article) author_wikipedia
INTO WIKIAUTHOR_DETAILS_TEMP
FROM wikiauthor_details w
group by author;

--Update author_info
UPDATE AUTHORS a
set author_name = a2.author_name, author_nationality = a2.author_nationality
	,author_positions = a2.author_positions
	,author_name_language = a2.author_name_language
	,author_birth_date = a2.author_birth_date
	,author_birth_city = a2.author_birth_city, author_birth_country = a2.author_birth_country,
	author_death_date = a2.author_death_date, author_death_city = a2.author_death_city,
	author_death_country = a2.author_death_country, author_death_year = a2.author_death_year,
	author_death_month = a2.author_death_month, author_death_day = a2.author_death_day,
	author_floruit = a2.author_floruit, author_wikipedia = a2.author_wikipedia
from (
select 
coalesce(a.author_name,a2.author_name) as author_name
,coalesce(a.author_citizenship,a2.author_nationality) as author_nationality
,coalesce(a.author_positions, a2.author_positions) as author_positions
,coalesce(a.author_name_language, a2.author_name_language) as author_name_language
,coalesce(a.author_birth_date, a2.author_birth_date) as author_birth_date
,case
	when coalesce(a.author_birth_city, a2.author_birth_city) like '%http://www.wikidata.org/.well-known/genid%' then null
	else coalesce(a.author_birth_city, a2.author_birth_city) 
	end as author_birth_city
,coalesce(a.author_birth_country, a2.author_birth_country) as author_birth_country
,coalesce(a.author_birth_year,a2.author_birth_year) as author_birth_year
,coalesce(a.author_birth_month::int, a2.author_birth_month) as author_birth_month
,coalesce(a.author_birth_day::int,a2.author_birth_day) as author_birth_day
,coalesce(a.author_death_date,a2.author_death_date) as author_death_date
,case
	when coalesce(a.author_death_city,a2.author_death_city) like '%http://www.wikidata.org/.well-known/genid%' then null
	else coalesce(a.author_death_city,a2.author_death_city) 
	end as author_death_city
,coalesce(a.author_death_country,a2.author_death_country) as author_death_country
,coalesce(a.author_death_year,a2.author_death_year) as author_death_year
,coalesce(a.author_death_month::int,a2.author_death_month) as author_death_month
,coalesce(a.author_death_day::int,a2.author_death_day) as author_death_day
,case 
	when coalesce(a.author_floruit,a2.author_floruit) like '%http://www.wikidata.org/.well-known/genid%' then null 
	else coalesce(a.author_floruit,a2.author_floruit) 
end as author_floruit
,a.author_q as author_q
,a.author_wikipedia as author_wikipedia
from WIKIAUTHOR_DETAILS_TEMP a
join authors a2 on a2.author_q = a.author_q
where coalesce(a.author_name,a2.author_name) is not null
) a2
where a2.author_q = a.author_q;
DROP TABLE IF EXISTS WIKIAUTHOR_DETAILS_TEMP;

---Wikitexts
DROP TABLE IF EXISTS WIKIAUTHOR_TEXTS_TEMP;
SELECT 	replace(text_q, 'http://www.wikidata.org/entity/', '') as text_q
	,string_agg(distinct replace(author, 'http://www.wikidata.org/entity/', ''),', ') as text_author_q
	,string_agg(distinct coalesce("bookLabel", "titleLabel"), ', ') as text_title
	,string_agg(distinct "akaLabel", ', ') as text_aka
	,string_agg(distinct "bookdesc", ', ') as text_description
	,string_agg(distinct "typeLabel", ', ') as text_type
	,string_agg(distinct "formLabel", ', ') as text_form
	,string_agg(distinct "genreLabel", ', ') as text_genre
	,string_agg(distinct "image", ', ') as text_image
	,max("publYear") as text_original_publication_year
	,max(publication) as text_original_publication_date
	,string_agg(distinct "languageLabel", ', ') as text_language
	,string_agg(distinct "origincountryLabel", ', ') as text_country
	,string_agg(distinct "publisherLabel", ', ') as text_original_publication_publisher
	,string_agg(distinct "lengthLabel", ', ') as text_length
	,string_agg(distinct "metreLabel", ', ') as text_meter
INTO WIKIAUTHOR_TEXTS_TEMP
from wikiauthor_texts
group by text_q;

---------Texts Detailed
DROP TABLE IF EXISTS WIKITEXT_DETAILS_TEMP;
SELECT text_q
		,string_agg(distinct author,', ') as text_author_q
		,max("bookLabel") as text_title
		,string_agg(distinct "authorLabel",', ') as text_author
		,string_agg(distinct image, ', ') as text_image_url
		,string_agg(distinct "akaLabel", ', ') as text_aka
		,string_agg(distinct bookdesc, ', ') as text_desc
		,string_agg(distinct "titleLabel", ', ') as text_original_title
		,string_agg(distinct "typeLabel", ', ') as text_type
		,string_agg(distinct "formLabel", ', ') as text_form
		,string_agg(distinct "genreLabel", ', ') as text_genre
		,max("publYear") as text_original_publication_year
		,max(case
				when left("publication",4) = 'http' then null
				when "publication" like '[A-Za-z]%' then null
				when "publication" like '%2SPA%' then null
				when left("publication",1) = '-' then right(left("publication",8),2)::int
				else right(left("publication",7),2)::int 
			end) as text_original_publication_month
		,max(case
				when left("publication",4) = 'http' then null
				when "publication" like '[A-Za-z]%' then null
				when "publication" like '%2SPA%' then null
				when left("publication",1) = '-' then right(left("publication",11),2)::int
				else right(left("publication",10),2)::int 
			end) as text_original_publication_day	
		,max(publication) as text_original_publication_date
		,string_agg(distinct "languageLabel", ', ') as text_language
		,string_agg(distinct "origincountryLabel", ', ') as text_origin_country
		,max("dopYear") as text_publication_year
		,max(inception) as text_inception_date
		,max("inceptionYear") as text_inception_year
		,string_agg(distinct "metreLabel", ', ') as text_metre
		,string_agg(distinct "publisherLabel", ', ') as text_original_publication_publisher
		,max("lengthLabel") text_length
		,string_agg(distinct "timeperiodLabel", ', ') as text_time_period
		,string_agg(distinct "awardsLabel", ', ') as text_awards
		,string_agg(distinct "publishedInLabel", ', ') as text_original_publication_publisher_loc
		,string_agg(distinct "copyrightLabel", ', ') as text_copyright
		,max(article) as text_wikipedia
INTO WIKITEXT_DETAILS_TEMP
FROM WIKITEXT_DETAILS
group by text_q;
---Inserting text detailed into texts
update texts t
set text_title = t2.text_title
	,text_author = t2.text_author,
	text_type = t2.text_type, text_language = t2.text_language,
	text_original_publication_date = t2.text_original_publication_date,
	text_original_publication_year = t2.text_original_publication_year,
	text_original_publication_month = t2.text_original_publication_month,
	text_original_publication_day = t2.text_original_publication_day,
	text_author_q = t2.text_author_q,text_wikipedia = t2.text_wikipedia
	,text_description = t2.text_description, text_image = t2.text_image
from (
select 
	coalesce(CASE 
			 	WHEN text_title like '%http://www.wikidata.org/.well-known%' then "text_original_title"
			 	ELSE "text_title" END,
			text_title)
			 as text_title
	,text_author
	,text_type
	,text_genre
	,text_language
	,case 
		when text_original_publication_date like '%http://www.wikidata.org/.well-known%' then null 
		else text_original_publication_date
	end as text_original_publication_date
	,text_original_publication_year
	,text_original_publication_month
	,text_original_publication_day
	,text_original_publication_publisher
	,text_original_publication_publisher_loc
	,text_length as text_original_publication_length
	--,null as text_original_publication_length_type
	--,null as text_writing_start
	--,null as text_writing_end
	,text_q
	,text_author_q
	,text_wikipedia
	,text_desc as text_description
	,text_image_url as text_image
from WIKITEXT_DETAILS_TEMP) t2
where t.text_q = t2.text_q and t.text_q is not null;
DROP TABLE IF EXISTS WIKITEXT_DETAILS_TEMP;

--Label simplifications for search
DROP TABLE IF EXISTS AUTHOR_LABELS, TEXT_LABELS;
SELECT 
author
,string_agg(distinct "authorLabel", ', ') author_label
INTO AUTHOR_LABELS
FROM WIKIAUTHOR_LABELS
group by author;
INSERT INTO TEXT_LABELS
SELECT 
text_q
,string_agg(distinct "textLabel", ', ') text_label
FROM WIKITEXT_LABELS l
left join TEXT_LABELS l2 on l2.text_q = l.text_q
group by text_q;
select 
