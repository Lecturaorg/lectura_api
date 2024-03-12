--Authors
drop table if exists authors_dim;
select 
	author_id
	,author_q
	--,author_label --*
	,o.occupations as author_positions --*
	,g.gender as author_gender --*
	,d.deathplace as author_death_city --*
	,d.deathplacecountry as author_death_country --*
	,b.pob as author_birth_city --*
	,b.sob as author_birth_state
	--,author_birth_country --*
	,l.languages as author_language
	--,author_externals --Unnecessary
	,author_birth_year
	,author_birth_month
	,author_death_year
	,author_death_month
	,author_floruit
into authors_dim
from authors a
--left join wikiauthor_labels l on a.author_q = l.author
left join authorbirth b on b.author = a.author_q
left join authordeath d on d.author = a.author_q
left join authoroccupations o on o.author = a.author_q
left join authorgender g on g.author = a.author_q
left join authorlanguages l on l.author = a.author_q
where b.pob is not null

select 
	a.author_id
	,a.author_q
	,max(author_birth_year) author_birth_year
	,max(author_death_year) author_death_year
	,max(author_floruit) author_floruit
	,string_agg(distinct o."occupationsLabel",', ') author_positions
	,string_agg(distinct g."gendersLabel",', ') author_gender
	,string_agg(distinct c."citiesLabel",', ') author_birth_city
	,string_agg(distinct c2."citiesLabel",', ') author_death_city
	,string_agg(distinct l."languagesLabel",', ') author_languages
	,string_agg(distinct co."countriesLabel",', ') author_birth_states
	,string_agg(distinct co2."countriesLabel",', ') author_death_country
from authors_dim a
left join occupations o on o.occupations = a.author_positions
left join genders g on g.genders = a.author_gender
left join cities c on c.cities = a.author_birth_city
left join cities c2 on c2.cities = a.author_death_city
left join languages l on l.languages = a.author_language
left join countries co on co.countries = a.author_birth_state
left join countries co2 on co2.countries = a.author_death_country
where o.lang = 'fr' and g.lang = 'fr' 
	and c.lang = 'fr' and c2.lang='fr' and l.lang = 'fr' and co.lang = 'fr'
	and co2.lang = 'fr'
group by a.author_id, a.author_q

select distinct author_birth_state as country from authors_dim where author_birth_state is not null
union distinct
select distinct author_death_country from authors_dim where author_death_country is not null

select distinct replace(author_birth_city,'http://www.wikidata.org/entity/', ' wd:') cities 
from authors_dim where author_birth_city is not null and author_birth_city not ilike '%http://www.wikidata.org/.well-known/genid%'
          union distinct
          select distinct replace(author_death_city,'http://www.wikidata.org/entity/', ' wd:') from authors_dim 
		  where author_death_city is not null and author_death_city not ilike '%http://www.wikidata.org/.well-known/genid%'

select distinct replace(author_gender,'http://www.wikidata.org/entity/', ' wd:') genders from authors_dim where author_gender is not null

select * from genders









