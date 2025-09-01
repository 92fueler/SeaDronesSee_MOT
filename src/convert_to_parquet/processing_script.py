"""
SeaDronesSee MOT JSON to Parquet Processing Script

This script converts SeaDronesSee MOT dataset JSON files to multiple parquet files
with partitioning strategy for efficient storage and querying.

The partitioning srategy is described in PARTITIONING_README.md
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys
import os
import pandas as pd
from datetime import datetime
import gc

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_json_file(file_path: Path) -> Dict[str, Any]:
    """
    Load and parse a JSON annotation file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Parsed JSON data as dictionary
    """
    try:
        logger.info(f"Loading JSON file: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Successfully loaded {file_path.name}")
        return data
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        raise


def process_categories(data: Dict[str, Any], output_dir: Path) -> None:
    """
    Process categories and save as parquet.
    
    Args:
        data: Parsed JSON data
        output_dir: Output directory for parquet files
    """
    logger.info("Processing categories...")
    
    categories = data.get('categories', [])
    if not categories:
        logger.warning("No categories found in data")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(categories)
    
    # Ensure proper column order and types
    df = df[['id', 'supercategory', 'name']].copy()
    df['id'] = df['id'].astype('int32')
    df['supercategory'] = df['supercategory'].astype('string')
    df['name'] = df['name'].astype('string')
    
    # Save as parquet (no partitioning for small dataset)
    output_file = output_dir / "categories.parquet"
    df.to_parquet(output_file, index=False)
    
    logger.info(f"Saved {len(df)} categories to {output_file}")
    logger.info(f"Categories: {df['name'].tolist()}")


def process_videos(data: Dict[str, Any], output_dir: Path) -> None:
    """
    Process videos and save as parquet.
    
    Args:
        data: Parsed JSON data
        output_dir: Output directory for parquet files
    """
    logger.info("Processing videos...")
    
    videos = data.get('videos', [])
    if not videos:
        logger.warning("No videos found in data")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(videos)
    
    # Ensure proper column order and types
    # Handle the 'name:' key (with colon) in the data
    if 'name:' in df.columns:
        df = df.rename(columns={'name:': 'name'})
    
    required_cols = ['id', 'height', 'width', 'name']
    available_cols = [col for col in required_cols if col in df.columns]
    df = df[available_cols].copy()
    
    df['id'] = df['id'].astype('int32')
    df['height'] = df['height'].astype('int32')
    df['width'] = df['width'].astype('int32')
    df['name'] = df['name'].astype('string')
    
    # Save as parquet (no partitioning for small dataset)
    output_file = output_dir / "videos.parquet"
    df.to_parquet(output_file, index=False)
    
    logger.info(f"Saved {len(df)} videos to {output_file}")


def process_images(data: Dict[str, Any], output_dir: Path) -> None:
    """
    Process images and save as partitioned parquet.
    
    Args:
        data: Parsed JSON data
        output_dir: Output directory for parquet files
    """
    logger.info("Processing images...")
    
    images = data.get('images', [])
    if not images:
        logger.warning("No images found in data")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(images)
    
    # Extract nested fields
    if 'source' in df.columns:
        source_df = pd.json_normalize(df['source'])
        df = pd.concat([df.drop('source', axis=1), source_df], axis=1)
    
    if 'meta' in df.columns:
        meta_df = pd.json_normalize(df['meta'])
        df = pd.concat([df.drop('meta', axis=1), meta_df], axis=1)
    
    # Ensure proper column order and types
    required_cols = ['id', 'file_name', 'height', 'width', 'video_id', 'frame_index']
    available_cols = [col for col in required_cols if col in df.columns]
    df = df[available_cols].copy()
    
    df['id'] = df['id'].astype('int32')
    df['height'] = df['height'].astype('int32')
    df['width'] = df['width'].astype('int32')
    df['video_id'] = df['video_id'].astype('int32')
    df['frame_index'] = df['frame_index'].astype('int32')
    df['file_name'] = df['file_name'].astype('string')
    
    # Save as partitioned parquet by video_id
    output_file = output_dir / "images.parquet"
    
    # Create descriptive filenames by manually creating partitions
    for video_id, group_df in df.groupby('video_id'):
        partition_dir = output_file / f"video_id={video_id}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        # Create descriptive filename
        filename = f"images_video_{video_id}.parquet"
        group_df.to_parquet(partition_dir / filename, index=False)
    
    logger.info(f"Saved {len(df)} images to {output_file} (partitioned by video_id)")
    logger.info(f"Number of video partitions: {df['video_id'].nunique()}")


def process_annotations(data: Dict[str, Any], output_dir: Path) -> None:
    """
    Process annotations and save as partitioned parquet.
    Optimized for re-identification (re-ID) and cross-video association using embeddings.
    
    Args:
        data: Parsed JSON data
        output_dir: Output directory for parquet files
    """
    logger.info("Processing annotations...")
    
    annotations = data.get('annotations', [])
    if not annotations:
        logger.warning("No annotations found in data")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(annotations)
    
    # Extract bbox coordinates
    if 'bbox' in df.columns:
        bbox_df = pd.DataFrame(df['bbox'].tolist(), columns=['bbox_x', 'bbox_y', 'bbox_width', 'bbox_height'])
        df = pd.concat([df.drop('bbox', axis=1), bbox_df], axis=1)
    
    # Ensure proper column order and types
    required_cols = ['id', 'image_id', 'category_id', 'video_id', 'track_id', 'area']
    available_cols = [col for col in required_cols if col in df.columns]
    df = df[available_cols].copy()
    
    df['id'] = df['id'].astype('int32')
    df['image_id'] = df['image_id'].astype('int32')
    df['category_id'] = df['category_id'].astype('int32')
    df['video_id'] = df['video_id'].astype('int32')
    df['track_id'] = df['track_id'].astype('int32')
    df['area'] = df['area'].astype('float32')
    
    # Add bbox columns if available
    bbox_cols = ['bbox_x', 'bbox_y', 'bbox_width', 'bbox_height']
    for col in bbox_cols:
        if col in df.columns:
            df[col] = df[col].astype('float32')
    
    # Save as partitioned parquet by category_id and track_id (optimized for re-ID)
    output_file = output_dir / "annotations.parquet"
    partition_cols = ['category_id', 'track_id']
    
    # Create descriptive filenames by manually creating partitions
    for (category_id, track_id), group_df in df.groupby(partition_cols):
        partition_dir = output_file / f"category_id={category_id}" / f"track_id={track_id}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        # Create descriptive filename
        filename = f"annotations_category_{category_id}_track_{track_id}.parquet"
        group_df.to_parquet(partition_dir / filename, index=False)
    
    logger.info(f"Saved {len(df)} annotations to {output_file}")
    logger.info(f"Partitioned by: {partition_cols} (optimized for re-ID)")
    logger.info(f"Number of partitions: {df.groupby(partition_cols).ngroups}")


def process_tracks(data: Dict[str, Any], output_dir: Path) -> None:
    """
    Process tracks and save as partitioned parquet.
    
    Args:
        data: Parsed JSON data
        output_dir: Output directory for parquet files
    """
    logger.info("Processing tracks...")
    
    tracks = data.get('tracks', [])
    if not tracks:
        logger.warning("No tracks found in data")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(tracks)
    
    # Ensure proper column order and types
    df = df[['id', 'category_id', 'video_id']].copy()
    df['id'] = df['id'].astype('int32')
    df['category_id'] = df['category_id'].astype('int32')
    df['video_id'] = df['video_id'].astype('int32')
    
    # Save as partitioned parquet by video_id
    output_file = output_dir / "tracks.parquet"
    
    # Create descriptive filenames by manually creating partitions
    for video_id, group_df in df.groupby('video_id'):
        partition_dir = output_file / f"video_id={video_id}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        # Create descriptive filename
        filename = f"tracks_video_{video_id}.parquet"
        group_df.to_parquet(partition_dir / filename, index=False)
    
    logger.info(f"Saved {len(df)} tracks to {output_file} (partitioned by video_id)")
    logger.info(f"Number of video partitions: {df['video_id'].nunique()}")


def save_processing_stats(data: Dict[str, Any], output_dir: Path) -> None:
    """
    Save processing statistics.
    
    Args:
        data: Parsed JSON data
        output_dir: Output directory for parquet files
    """
    stats = {
        "processing_timestamp": datetime.now().isoformat(),
        "categories_count": len(data.get('categories', [])),
        "images_count": len(data.get('images', [])),
        "annotations_count": len(data.get('annotations', [])),
        "videos_count": len(data.get('videos', [])),
        "tracks_count": len(data.get('tracks', [])),
        "licenses_count": len(data.get('licenses', [])),
        "info": data.get('info', {})
    }
    
    # Add video-specific stats
    if 'videos' in data:
        videos_df = pd.DataFrame(data['videos'])
        stats["unique_video_ids"] = videos_df['id'].nunique() if 'id' in videos_df.columns else 0
    
    # Add category-specific stats
    if 'categories' in data:
        categories_df = pd.DataFrame(data['categories'])
        stats["unique_supercategories"] = categories_df['supercategory'].nunique() if 'supercategory' in categories_df.columns else 0
    
    stats_file = output_dir / "processing_stats.json"
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    
    logger.info(f"Saved processing statistics to {stats_file}")


def main():
    """
    Main function to process JSON files and convert to parquet.
    """
    try:
        logger.info("Starting JSON to Parquet conversion")
        
        # Define paths
        project_root = Path(__file__).parent.parent.parent
        annotations_dir = project_root / "data" / "annotations"
        output_dir = project_root / "data" / "parquet"
        
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Process only instances_train_objects_in_water.json
        json_file = annotations_dir / "instances_train_objects_in_water.json"
        if not json_file.exists():
            logger.error(f"File not found: {json_file}")
            return
        
        try:
            logger.info(f"Processing {json_file.name}")
            
            # Load JSON data
            data = load_json_file(json_file)
            
            # Process each data type with Hive convention structure
            process_categories(data, output_dir)
            process_videos(data, output_dir)
            process_images(data, output_dir)
            process_annotations(data, output_dir)
            process_tracks(data, output_dir)
            
            # Save processing statistics
            save_processing_stats(data, output_dir)
            
            # Clear memory
            del data
            gc.collect()
            
            logger.info(f"Completed processing {json_file.name}")
            
        except Exception as e:
            logger.error(f"Error processing {json_file.name}: {e}")
            raise
        
        logger.info("JSON to Parquet conversion completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        raise


if __name__ == "__main__":
    main()
