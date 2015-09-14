
CREATE TABLE IF NOT EXISTS items (
  id INT PRIMARY KEY NOT NULL,
  promoted INT NOT NULL,
  up INT NOT NULL,
  down INT NOT NULL,
  created INT NOT NULL,
  image TEXT NOT NULL,
  thumb TEXT NOT NULL,
  fullsize TEXT NOT NULL,
  source TEXT NOT NULL,
  flags INT NOT NULL,
  username TEXT NOT NULL,
  mark INT NOT NULL
);

CREATE TABLE IF NOT EXISTS sizes (
  id INT PRIMARY KEY NOT NULL,
  width INT NOT NULL,
  height INT NOT NULL,

  FOREIGN KEY (id) REFERENCES items(id)
);

CREATE TABLE IF NOT EXISTS tags (
  id INT PRIMARY KEY NOT NULL,
  item_id INT NOT NULL,
  confidence REAL NOT NULL,
  tag TEXT NOT NULL,

  FOREIGN KEY (item_id) REFERENCES items(id)
);

CREATE TABLE users (
  id INT PRIMARY KEY,
  name TEXT,
  registered INT,
  score INT
);

CREATE TABLE user_score (
  user_id INT,
  timestamp INT,
  score INT,

  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE controversial (
  id SERIAL PRIMARY KEY,
  item_id INTEGER UNIQUE NOT NULL,

  FOREIGN KEY (item_id) REFERENCES items(id)
);


CREATE INDEX tags__item_id ON tags(item_id);
CREATE INDEX users__name ON users(lower(name));
CREATE INDEX user_score__user_id__timestamp ON user_score(user_id, timestamp);
