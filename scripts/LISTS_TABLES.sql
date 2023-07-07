DROP TABLE IF EXISTS USER_LISTS;
CREATE TABLE USER_LISTS (
	list_id SERIAL PRIMARY KEY,
	list_name varchar(255),
	list_description varchar(255),
	list_type varchar(100),
	list_created date DEFAULT now(),
	list_modified date DEFAULT now(),
	user_id INTEGER NOT NULL,
	list_deleted BOOLEAN DEFAULT NULL
);

CREATE TABLE OFFICIAL_LISTS (
	list_id SERIAL PRIMARY KEY,
	list_name varchar(255),
	list_description varchar(255),
	list_type varchar(100),
	list_url varchar(50)
);

DROP TABLE IF EXISTS USER_LISTS_ELEMENTS;
CREATE TABLE USER_LISTS_ELEMENTS (
    element_id SERIAL PRIMARY KEY,
    list_id INTEGER NOT NULL,
    FOREIGN KEY (list_id) REFERENCES USER_LISTS(list_id),
    value INTEGER NOT NULL,
	element_added date DEFAULT now(),
	order_rank INTEGER NOT NULL
);

CREATE TABLE USER_LISTS_WATCHLISTS (
	watchlist_id SERIAL PRIMARY KEY,
	list_id INTEGER NOT NULL,
	--FOREIGN KEY(list_id) REFERENCES USER_LISTS(list_id),
	user_id INTEGER NOT NULL,
	FOREIGN KEY(user_id) REFERENCES USERS (user_id),
	watchlist_date TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE USER_LISTS_LIKES (
	list_like_id SERIAL PRIMARY KEY,
	list_id INTEGER NOT NULL,
	--FOREIGN KEY(list_id) REFERENCES USER_LISTS(list_id),
	user_id INTEGER NOT NULL,
	FOREIGN KEY(user_id) REFERENCES USERS (user_id),
	like_date TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE USER_LISTS_DISLIKES (
	list_dislike_id SERIAL PRIMARY KEY,
	list_id INTEGER NOT NULL,
	--FOREIGN KEY(list_id) REFERENCES USER_LISTS(list_id),
	user_id INTEGER NOT NULL,
	FOREIGN KEY(user_id) REFERENCES USERS (user_id),
	dislike_date TIMESTAMPTZ DEFAULT now()
);
