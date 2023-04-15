CREATE TABLE comments (
  id SERIAL PRIMARY KEY,
  comment TEXT NOT NULL,
  user_id INTEGER NOT NULL,
  date TIMESTAMP NOT NULL
);

CREATE TABLE comment_replies (
  id SERIAL PRIMARY KEY,
  parent_comment_id INTEGER NOT NULL,
  comment TEXT NOT NULL,
  user_id INTEGER NOT NULL,
  date TIMESTAMP NOT NULL,
  likes INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (parent_comment_id) REFERENCES comments(id)
);

CREATE TABLE comment_likes (
  id SERIAL PRIMARY KEY,
date TIMESTAMP NOT NULL,
  comment_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  FOREIGN KEY (comment_id) REFERENCES comments(id),
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE comment_reply_likes (
  id SERIAL PRIMARY KEY,
  comment_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  FOREIGN KEY (comment_id) REFERENCES comment_replies(id),
  FOREIGN KEY (user_id) REFERENCES users(id)
);
