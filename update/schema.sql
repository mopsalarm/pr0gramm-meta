
CREATE TABLE IF NOT EXISTS items (
  id INT PRIMARY KEY,
  promoted INT,
  up INT,
  down INT,
  created INT,
  image TEXT,
  thumb TEXT,
  fullsize TEXT,
  source TEXT,
  flags INT,
  username TEXT,
  mark INT
);

CREATE TABLE IF NOT EXISTS sizes (
  id INT PRIMARY KEY,
  width INT,
  height INT
);

CREATE TABLE IF NOT EXISTS tags (
  id INT PRIMARY KEY,
  item_id INT,
  confidence REAL,
  tag TEXT,
  FOREIGN KEY (item_id) REFERENCES items(id)
);

CREATE INDEX tags_item_id ON tags(item_id);
