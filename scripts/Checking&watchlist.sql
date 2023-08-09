CREATE TABLE CHECKS (
	check_id SERIAL PRIMARY KEY,
	text_id INTEGER REFERENCES texts (text_id),
	user_id INTEGER REFERENCES users (user_id),
	check_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE WATCH (
	watch_id SERIAL PRIMARY KEY,
	text_id INTEGER REFERENCES texts (text_id),
	user_id INTEGER REFERENCES users (user_id),
	watch_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE AUTHOR_WATCH (
	author_watch_id SERIAL PRIMARY KEY,
	author_id INTEGER REFERENCES AUTHORS (author_id),
	user_id INTEGER REFERENCES USERS (user_id),
	watch_date TIMESTAMPTZ DEFAULT NOW()
);