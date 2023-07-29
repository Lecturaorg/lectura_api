SELECT 
    element_id
    ,list_id
    ,l.value
    ,a.author_name
    ,a.author_positions
    ,a.author_birth_year
    ,a.author_death_year
    ,a.author_birth_city
    ,a.author_birth_country
    ,a.author_death_city
    ,a.author_death_country
    ,CONCAT(
    SPLIT_PART(author_name, ', ', 1),
    COALESCE(
        CASE
        WHEN author_birth_year IS NULL AND author_death_year IS NULL AND author_floruit IS NULL THEN ''
        WHEN author_birth_year IS NULL AND author_death_year IS NULL THEN CONCAT(' (fl.', left(author_floruit,4), ')')
        WHEN author_birth_year IS NULL THEN CONCAT(' (d.', 
                CASE 
                    WHEN author_death_year<0 THEN CONCAT(ABS(author_death_year)::VARCHAR, ' BC')
                    ELSE CONCAT(author_death_year::VARCHAR, ' AD')
                END
            ,
            ')')
        WHEN author_death_year IS NULL THEN CONCAT(' (b.', 
            CASE
                WHEN author_birth_year<0 THEN CONCAT(ABS(author_birth_year)::VARCHAR, ' BC')
                ELSE concat(author_birth_year::VARCHAR, ' AD')
            END
            ,
            ')')
        ELSE CONCAT(' (', ABS(author_birth_year), '-',
            CASE 
                WHEN author_death_year<0 THEN CONCAT(ABS(author_death_year)::VARCHAR, ' BC')
                ELSE CONCAT(author_death_year::VARCHAR, ' AD')
            END
            ,
            ')')
        END,
        ''
    )
    ) AS label
    ,case when w.text_id is not null then true else false end as watch
FROM USER_LISTS_ELEMENTS L
join authors a on a.author_id = l.value
left join (select distinct text_id from watch where user_id = [$user_id]) w on w.text_id = L.element_id
WHERE LIST_ID = [@list_id]