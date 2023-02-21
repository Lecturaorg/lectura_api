with upd as (
WITH positions as (
select 
	string_to_array(lower(author_positions), ', ') positions_array
	, author_id
from authors
) 
select a.author_id,
	unnest(case
		when array_length(positions_array,1) >1 then array_append(positions_array[2:array_length(positions_array,1)],
																  case 
												when split_part(positions_array[1], ' ',1) != '' 
													then replace(positions_array[1],split_part(positions_array[1],' ',1),'')
												else positions_array[1]
												end)
		else array_append('{}'::text[],case 
		when split_part(positions_array[1], ' ',1) != '' 
			then replace(positions_array[1],split_part(positions_array[1],' ',1),'')
		else positions_array[1]
		end)
	end)
	as author_occupation
from authors a
left join positions p on p.author_id = a.author_id
)
--INSERT INTO AUTHOR_OCCUPATIONS (author_id, author_occupation) select * from upd;
--INSERT INTO AUTHOR_OCCUPATIONS


--commit;