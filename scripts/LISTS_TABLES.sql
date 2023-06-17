DROP TABLE IF EXISTS USER_LISTS;
CREATE TABLE USER_LISTS (
	list_id SERIAL PRIMARY KEY,
	list_name varchar(255),
	list_description varchar(255),
	list_type varchar(100),
	list_created date DEFAULT now(),
	list_modified date DEFAULT now(),
	user_id INTEGER NOT NULL
);
/*CREATE TABLE USER_LISTS_DETAIL (
	list_id INTEGER NOT NULL,
	SELECT * from user_lists
	select * from users
)*/

DROP TABLE IF EXISTS USER_LISTS_ELEMENTS;
CREATE TABLE USER_LISTS_ELEMENTS (
    element_id SERIAL PRIMARY KEY,
    list_id INTEGER NOT NULL,
    FOREIGN KEY (list_id) REFERENCES USER_LISTS(list_id),
    value INTEGER NOT NULL,
	element_added date DEFAULT now()
);

--CREATE TABLE USER_LISTS

