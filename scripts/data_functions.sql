BEGIN;
DROP FUNCTION IF EXISTS getTexts;
CREATE FUNCTION getTexts(int) RETURNS TABLE (text_id int, label varchar(255)) AS $$
    SELECT text_id
		,text_title || 
			CASE
				WHEN TEXT_ORIGINAL_publication_year is null then ''
				when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC)'
				else ' (' || text_original_publication_year || ' AD)'
			end
			as label 
	FROM texts WHERE author_id = $1;
$$ LANGUAGE SQL;
COMMIT;

BEGIN;
DROP FUNCTION IF EXISTS getEditions;
CREATE FUNCTION getEditions(int) RETURNS TABLE (edition_id int, edition_title varchar(255), label varchar(255)) AS $$
    SELECT 
		edition_id
		,edition_title
		,case 
			when edition_publication_year is null then ''
			else ' (published ' || edition_publication_year || ')'
		end
		||
		case
			when edition_editor is null then ''
			else ' (editors: ' || edition_editor || ')'
		end
		||
		case
			when edition_language is null then ''
			else ' (language: ' || edition_language || ')'
		end
		||
		case
			when edition_isbn is null then ''
			else ' (ISBN ' || edition_isbn || '/' || edition_isbn13 || ')'
		end as label	FROM editions WHERE text_id = $1;
$$ LANGUAGE SQL;
COMMIT;
