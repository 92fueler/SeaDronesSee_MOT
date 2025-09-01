# SeaDronesSee MOT JSON to Parquet Processing

This script converts SeaDronesSee MOT dataset JSON files to multiple parquet files with smart partitioning for efficient storage and querying.

## Output Structure

```
data/parquet/
├── categories.parquet                    # No partitioning (small dataset)
├── videos.parquet                        # No partitioning (small dataset)
├── images.parquet/                       # Partitioned by video_id
│   ├── video_id=0/
│   ├── video_id=1/
│   └── ...
├── annotations.parquet/                  # Partitioned by category_id + track_id
│   ├── category_id=1/
│   │   ├── track_id=0/
│   │   ├── track_id=1/
│   │   └── ...
│   ├── category_id=2/
│   │   ├── track_id=5/
│   │   └── ...
│   └── ...
└── tracks.parquet/                       # Partitioned by track_id
    ├── track_id=0/
    ├── track_id=1/
    └── ...
```

## Partitioning Strategies

### 1. Categories Table
- **Partitioning**: None
- **Reasoning**: Small dataset (only 3 categories), no natural grouping needed
- **File**: `categories.parquet`

### 2. Videos Table  
- **Partitioning**: None
- **Reasoning**: Small dataset (~20 videos), no natural grouping needed
- **File**: `videos.parquet`

### 3. Images Table
- **Partitioning**: `video_id`
- **Reasoning**: Images belong to videos, natural grouping for video-based queries
- **Structure**: `images.parquet/video_id={id}/`
- **Benefits**: Efficient filtering by video, sequential access to frames

### 4. Annotations Table
- **Partitioning**: `category_id` + `track_id`
- **Reasoning**: Optimized for re-identification (re-ID) and cross-video association using embeddings
- **Structure**: `annotations.parquet/category_id={id}/track_id={id}/`
- **Benefits**: Efficient vector search within object categories, complete object trajectories for temporal analysis

### 5. Tracks Table
- **Partitioning**: `track_id`
- **Reasoning**: Tracks represent object trajectories, natural grouping by track
- **Structure**: `tracks.parquet/track_id={id}/`
- **Benefits**: Efficient access to complete object trajectories
