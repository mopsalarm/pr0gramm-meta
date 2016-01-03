BEGIN;

CREATE TABLE IF NOT EXISTS items (
  id       INT PRIMARY KEY NOT NULL,
  promoted INT             NOT NULL,
  up       INT             NOT NULL,
  down     INT             NOT NULL,
  created  INT             NOT NULL,
  image    TEXT            NOT NULL,
  thumb    TEXT            NOT NULL,
  fullsize TEXT            NOT NULL,
  source   TEXT            NOT NULL,
  flags    INT             NOT NULL,
  username TEXT            NOT NULL,
  mark     INT             NOT NULL
);

CREATE TABLE IF NOT EXISTS sizes (
  id     INT PRIMARY KEY NOT NULL REFERENCES items (id),
  width  INT             NOT NULL,
  height INT             NOT NULL
);

CREATE TABLE IF NOT EXISTS item_previews (
  id      INT PRIMARY KEY NOT NULL REFERENCES items (id),
  width   INT             NOT NULL,
  height  INT             NOT NULL,
  preview BYTEA           NOT NULL
);

CREATE TABLE IF NOT EXISTS tags (
  id         INT PRIMARY KEY NOT NULL,
  item_id    INT             NOT NULL REFERENCES items (id),
  confidence REAL            NOT NULL,
  tag        TEXT            NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
  id         INT  NOT NULL PRIMARY KEY,
  name       TEXT NOT NULL,
  registered INT  NOT NULL,
  score      INT  NOT NULL
);

CREATE TABLE IF NOT EXISTS user_score (
  user_id   INT NOT NULL REFERENCES users (id),
  timestamp INT NOT NULL,
  score     INT NOT NULL
);

CREATE TABLE IF NOT EXISTS controversial (
  id      SERIAL PRIMARY KEY,
  item_id INT UNIQUE NOT NULL REFERENCES items (id)
);

CREATE TABLE IF NOT EXISTS items_bestof (
  id    INT PRIMARY KEY REFERENCES items (id),
  score INT NOT NULL
);

CREATE INDEX IF NOT EXISTS tags__item_id ON tags(item_id);
CREATE INDEX IF NOT EXISTS tags__tag_repost ON tags(lower("tag")) where lower("tag")='repost';
CREATE INDEX IF NOT EXISTS tags__tag_full ON tags USING GIN (to_tsvector('simple', tags.tag));
CREATE INDEX IF NOT EXISTS users__name ON users(lower("name") text_pattern_ops);
CREATE INDEX IF NOT EXISTS user_score__user_id__timestamp ON user_score(user_id, "timestamp");
CREATE INDEX IF NOT EXISTS items_bestof__score ON items_bestof(score);

-- not used right now
-- CREATE INDEX IF NOT EXISTS items__username ON items(lower(username));
-- CREATE INDEX IF NOT EXISTS tags__tag ON tags(lower("tag"));

-- The update function that modifies the items in the items_bestof table.
CREATE OR REPLACE FUNCTION pr0_update_item_score()
  RETURNS TRIGGER AS $pr0_update_item_score$
BEGIN
  -- update score
  IF NEW.up - NEW.down >= 500
  THEN
    INSERT INTO items_bestof (id, score)
    VALUES (NEW.id, NEW.up - NEW.down) ON CONFLICT(id) DO UPDATE SET score=NEW.up-NEW.down;
  ELSE
    DELETE FROM items_bestof WHERE id = NEW.id;
  END IF;

  -- update controversial table.
  IF NEW.up>60 AND NEW.down>60 AND least(NEW.up, NEW.down)::float/greatest(NEW.up, NEW.down)>=0.7
  THEN
    INSERT INTO controversial (item_id) VALUES (NEW.id) ON CONFLICT(item_id) DO NOTHING;
  END IF;

  RETURN NULL;
END;
$pr0_update_item_score$ LANGUAGE plpgsql;

-- Drop previous trigger and recreate it
DROP TRIGGER IF EXISTS items__update_score ON items;
CREATE TRIGGER items__update_score AFTER INSERT OR UPDATE OF up, down ON items
FOR EACH ROW EXECUTE PROCEDURE pr0_update_item_score();

END;
