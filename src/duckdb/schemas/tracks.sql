CREATE TABLE IF NOT EXISTS tracks (
  id          BIGINT PRIMARY KEY,  
  category_id INTEGER NOT NULL,    
  video_id    BIGINT NOT NULL,     
  FOREIGN KEY (video_id) REFERENCES videos(id)
);
