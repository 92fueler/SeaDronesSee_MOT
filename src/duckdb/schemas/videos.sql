CREATE TABLE IF NOT EXISTS videos (
  id          BIGINT PRIMARY KEY,  -- Match the JSON video_id type
  name        TEXT NOT NULL,       -- Video filename/path (from JSON "name:" field)
  height      INTEGER NOT NULL,    -- Video resolution height
  width       INTEGER NOT NULL,    -- Video resolution width
  UNIQUE(name)
);
