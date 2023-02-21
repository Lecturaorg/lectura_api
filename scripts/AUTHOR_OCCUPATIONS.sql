CREATE TABLE AUTHOR_OCCUPATIONS (
	occupation_id SERIAL PRIMARY KEY,
	author_id int REFERENCES AUTHORS (author_id),
	author_occupation varchar(255)
);