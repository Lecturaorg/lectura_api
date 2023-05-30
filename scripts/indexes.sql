CREATE INDEX idx_texts_full_text_search ON texts USING gin(to_tsvector('english', immutable_concat_ws('',ARRAY[text_title,text_author])));

CREATE FUNCTION immutable_concat_ws(delimiter text, texts text[])
  RETURNS text
  IMMUTABLE
  LANGUAGE SQL
AS $$
  SELECT array_to_string(texts, delimiter);
$$;
CREATE INDEX idx_authors_full_text_search ON authors
  USING gin(to_tsvector('english', immutable_concat_ws(' ', ARRAY[coalesce(author_name,''), coalesce(author_nationality,''), coalesce(author_positions,''), coalesce(author_birth_city,''), coalesce(author_birth_country,''), coalesce(author_name_language,'')])));
