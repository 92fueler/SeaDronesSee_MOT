CREATE TABLE IF NOT EXISTS images (
  id         BIGINT PRIMARY KEY,
  file_name  TEXT NOT NULL,
  file_path  TEXT,  -- Full path to image file
  date_time  TIMESTAMP,  -- From JSON date_time field
  height     INTEGER NOT NULL,
  width      INTEGER NOT NULL,
  video_id   BIGINT REFERENCES videos(id),  -- Match videos table PK type
  frame_index INTEGER,
  dataset_split TEXT,  -- 'train', 'val', 'test'
  source     JSONB,  -- From JSON source field
  meta       JSONB,  -- From JSON meta field
  UNIQUE(file_name)
);
