CREATE TABLE IF NOT EXISTS annotations (
  id          INTEGER PRIMARY KEY, 
  image_id    INTEGER NOT NULL,     
  bbox_x      INTEGER NOT NULL,    -- Bounding box x coordinate (COCO format)
  bbox_y      INTEGER NOT NULL,    -- Bounding box y coordinate (COCO format)
  bbox_width  INTEGER NOT NULL,    -- Bounding box width
  bbox_height INTEGER NOT NULL,    -- Bounding box height
  area        INTEGER NOT NULL,    -- Area in pixels
  category_id INTEGER NOT NULL,  
  video_id    INTEGER NOT NULL,     
  track_id    INTEGER NOT NULL,    
  FOREIGN KEY (image_id) REFERENCES images(id),
  FOREIGN KEY (video_id) REFERENCES videos(id)
);
