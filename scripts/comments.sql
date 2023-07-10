CREATE TABLE comments (
	comment_id SERIAL PRIMARY KEY,
	user_id INTEGER REFERENCES users(user_id),
	parent_comment_id INTEGER REFERENCES comments(comment_id),
	comment_type varchar(50) NOT NULL, --list
	comment_type_id INTEGER,
	comment_content TEXT,
	comment_created_at TIMESTAMPTZ DEFAULT NOW(),
	comment_edited_at TIMESTAMPTZ,
	comment_likes INTEGER DEFAULT 0,
	comment_dislikes INTEGER DEFAULT 0,
	comment_deleted DEFAULT NULL
);

CREATE TABLE comment_ratings (
	comment_rating_id SERIAL PRIMARY KEY,
	user_id INTEGER REFERENCES users(user_id),
	comment_id INTEGER REFERENCES comments(comment_id),
	comment_rating_type VARCHAR(100), -- 'like' or 'dislike'
	comment_rating_created_at TIMESTAMPTZ DEFAULT NOW()
);