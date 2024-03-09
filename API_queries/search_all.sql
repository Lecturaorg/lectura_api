SET statement_timeout = 60000;
-- Combine subqueries into a single query with conditional logic
SELECT
    value,
    type,
    CASE
        WHEN a.author_id IS NOT NULL THEN a.author_id::varchar(255)
        ELSE NULL
    END AS author_id,
    label
FROM (
    SELECT
        text_id AS value,
        'text' AS type,
        text_title || CASE
            WHEN text_original_publication_year IS NULL THEN ' - '
            WHEN text_original_publication_year < 0 THEN ' (' || abs(text_original_publication_year) || ' BC' || ') - '
            ELSE ' (' || text_original_publication_year || ' AD' || ') - '
        END || COALESCE(text_author, 'Unknown') AS label,
        text_author_q
    FROM texts t
    WHERE text_title ILIKE %(query)s OR text_author ILIKE %(query)s
) t
LEFT JOIN (
    SELECT DISTINCT author_q, author_id
    FROM authors
) a ON a.author_q = t.text_author_q
UNION ALL
SELECT
    value,
    type,
    author_id,
    label
FROM (
    SELECT
        author_id AS value,
        'author' AS type,
        NULL AS author_id,
        CONCAT(
            SPLIT_PART(author_name, ', ', 1),
            COALESCE(
                CASE
                    WHEN author_birth_year IS NULL AND author_death_year IS NULL AND author_floruit IS NULL THEN ''
                    WHEN author_birth_year IS NULL AND author_death_year IS NULL THEN CONCAT(' (fl.', LEFT(author_floruit, 4), ')')
                    WHEN author_birth_year IS NULL THEN CONCAT(' (d.', CASE WHEN author_death_year < 0 THEN CONCAT(ABS(author_death_year)::VARCHAR, ' BC') ELSE CONCAT(author_death_year::VARCHAR, ' AD') END, ')')
                    WHEN author_death_year IS NULL THEN CONCAT(' (b.', CASE WHEN author_birth_year < 0 THEN CONCAT(ABS(author_birth_year)::VARCHAR, ' BC') ELSE CONCAT(author_birth_year::VARCHAR, ' AD') END, ')')
                    ELSE CONCAT(' (', ABS(author_birth_year), '-', CASE WHEN author_death_year < 0 THEN CONCAT(ABS(author_death_year)::VARCHAR, ' BC') ELSE CONCAT(author_death_year::VARCHAR, ' AD') END, ')')
                END,
                ''
            )
        ) AS label
    FROM authors a
    LEFT JOIN AUTHOR_LABELS lab ON lab.author = a.author_q
    WHERE author_name ILIKE %(query)s
        OR lab.author_label ILIKE %(query)s
        OR author_nationality ILIKE %(query)s
        OR author_positions ILIKE %(query)s
        OR author_birth_city ILIKE %(query)s
        OR author_birth_country ILIKE %(query)s
        OR author_name_language ILIKE %(query)s
) ag;
