--Authors
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
from authors a
--left join wikiauthor_labels l on a.author_q = l.author
left join authorbirth b on b.author = a.author_q
left join authordeath d on d.author = a.author_q
left join authoroccupations o on o.author = a.author_q
left join authorgender g on g.author = a.author_q
left join authorlanguages l on l.author = a.author_q
where b.pob is not null








