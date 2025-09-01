CREATE TABLE IF NOT EXISTS tracks (
  id          INTEGER PRIMARY KEY,  
  category_id INTEGER NOT NULL,    
  video_id    INTEGER NOT NULL,     
  FOREIGN KEY (video_id) REFERENCES videos(id)
);
