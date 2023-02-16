CREATE TABLE edits (
	edit_id SERIAL PRIMARY KEY
	,id int
	,type text CHECK (char_length(type)<=255)
	,variable text CHECK (char_length(variable)<=255)
	,value text CHECK (char_length(value)<=255)
	,editDate TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

