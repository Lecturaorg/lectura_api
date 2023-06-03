    SET statement_timeout = 60000; 
    SELECT 
    a.author_id as author_id
    ,author_nationality as nationality
    ,author_name_language as language
    ,MAX(author_q) as author_q
    ,MAX(author_birth_year) author_birth_year
    ,max(author_death_year) author_death_year
    ,max(author_positions) author_positions
    ,(case
        --when left(min("author_floruit"),4) = 'http' then null
        when left(min("author_floruit"::varchar(255)),1) = '-' then left(min("author_floruit"::varchar(255)),5)
        else left(min("author_floruit"::varchar(255)),4)
    end) as floruit
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
    ,COUNT(DISTINCT t.text_id) texts
    from authors a
    join texts t on t.author_id = a.author_id::varchar(255)
    where a.author_nationality ilike '%[country]%' and (t.text_language ilike '%[language]%' 
    or (t.text_language is null and a.author_name_language = '[language]')) 
    and a.author_positions not ILIKE '%school inspector%'
    group by a.author_id, author_birth_year, author_death_year, author_floruit,author_nationality,author_name_language
