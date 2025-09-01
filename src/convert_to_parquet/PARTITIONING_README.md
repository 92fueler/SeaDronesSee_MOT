# SeaDronesSee MOT Parquet Partitioning Strategy

This document describes the partitioning strategy used for converting SeaDronesSee MOT JSON annotations to Parquet format for efficient storage and querying.

## Overview

The SeaDronesSee MOT dataset is converted from JSON format to partitioned Parquet files to optimize:
- **Storage efficiency**: Compressed columnar format
- **Query performance**: Partition pruning for filtered queries
- **Scalability**: Parallel processing capabilities
- **Memory usage**: Reduced memory footprint during processing

## Partitioning Structure

```
data/parquet/
├── categories.parquet                    # No partitioning (small dataset, 4 categories)
├── videos.parquet                        # No partitioning (small dataset, 20 videos)
├── images.parquet/                       # Partitioned by video_id
│   ├── video_id=0/
│   │   └── video_0_images.parquet
│   ├── video_id=1/
│   │   └── video_1_images.parquet
│   ├── video_id=2/
│   │   └── video_2_images.parquet
│   └── ... (20 video partitions total)
├── annotations.parquet/                  # Partitioned by category_id + track_id
│   ├── category_id=1/
│   │   ├── track_id=3/
│   │   │   └── category_1_track_3_annotations.parquet
│   │   ├── track_id=11/
│   │   │   └── category_1_track_11_annotations.parquet
│   │   ├── track_id=33/
│   │   │   └── category_1_track_33_annotations.parquet
│   │   └── ... (50+ track partitions for category 1)
│   ├── category_id=2/
│   │   ├── track_id=5/
│   │   │   └── category_2_track_5_annotations.parquet
│   │   └── ... (track partitions for category 2)
│   ├── category_id=3/
│   │   └── ... (track partitions for category 3)
│   └── category_id=6/
│       └── ... (track partitions for category 6)
└── tracks.parquet/                       # Partitioned by category_id
    ├── category_id=1/
    │   └── category_1_tracks.parquet
    ├── category_id=2/
    │   └── category_2_tracks.parquet
    ├── category_id=3/
    │   └── category_3_tracks.parquet
    └── category_id=6/
        └── category_6_tracks.parquet
```

## Partitioning Rationale

### Categories & Videos (No Partitioning)
- **Categories**: Only 4 categories total, partitioning overhead not justified
- **Videos**: Only 20 videos total, small enough to keep as single files
- **Benefits**: Simple queries, no partition management overhead

### Images (Partitioned by video_id)
- **Partition Key**: `video_id`
- **Rationale**: 
  - 27,259 images across 20 videos
  - Queries often filter by video sequence
  - Enables efficient video-based analysis
  - Natural temporal grouping within videos
- **File Naming**: `video_{video_id}_images.parquet`

### Annotations (Partitioned by category_id + track_id)
- **Partition Keys**: `category_id`, `track_id`
- **Rationale**:
  - 160,470 annotations across 323 tracks
  - Queries often filter by object category and/or specific tracks
  - Enables efficient track-based analysis
  - Supports category-specific processing pipelines
- **File Naming**: `category_{category_id}_track_{track_id}_annotations.parquet`

### Tracks (Partitioned by category_id)
- **Partition Key**: `category_id`
- **Rationale**:
  - 323 tracks across 4 categories
  - Optimized for global track fusion across videos
  - Category-based analysis is common
  - Smaller partition count for better management
- **File Naming**: `category_{category_id}_tracks.parquet`

## Dataset Statistics

Based on processing results:
- **Categories**: 4 unique categories across 3 supercategories
- **Videos**: 20 unique video sequences
- **Images**: 27,259 images total
- **Annotations**: 160,470 bounding box annotations
- **Tracks**: 323 unique object tracks
- **Partitions**: 
  - Images: 20 partitions (by video_id)
  - Annotations: ~50+ partitions per category (by track_id)
  - Tracks: 4 partitions (by category_id)

