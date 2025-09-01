"""
SeaDronesSee MOT JSON to Parquet Processing Script

This script converts SeaDronesSee MOT dataset JSON files to multiple parquet files
with partitioning strategy for efficient storage and querying.

The partitioning srategy is described in PARTITIONING_README.md
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any
import sys
import os

import pandas as pd
import pyarrow as pa
from datetime import datetime
import gc

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


# PyArrow schema definitions for each table
PARQUET_SCHEMAS = {
    'categories': pa.schema([
        pa.field('id', pa.int32(), nullable=False),           # INTEGER PRIMARY KEY
        pa.field('supercategory', pa.string(), nullable=True), # TEXT
        pa.field('name', pa.string(), nullable=True)          # TEXT
    ]),
    
    'videos': pa.schema([
        pa.field('id', pa.int32(), nullable=False),           # INTEGER PRIMARY KEY
        pa.field('height', pa.int32(), nullable=False),       # INTEGER
        pa.field('width', pa.int32(), nullable=False),        # INTEGER
        pa.field('name', pa.string(), nullable=False)         # TEXT
    ]),
    
    'images': pa.schema([
        pa.field('id', pa.int32(), nullable=False),           # INTEGER PRIMARY KEY
        pa.field('file_name', pa.string(), nullable=False),   # TEXT
        pa.field('file_path', pa.string(), nullable=True),    # TEXT - Full path to image file
        pa.field('date_time', pa.string(), nullable=True),    # TEXT - Keep as string for now
        pa.field('height', pa.int32(), nullable=False),       # INTEGER
        pa.field('width', pa.int32(), nullable=False),        # INTEGER
        pa.field('video_id', pa.int32(), nullable=False),     # INTEGER FOREIGN KEY
        pa.field('frame_index', pa.int32(), nullable=True),   # INTEGER
        pa.field('dataset_split', pa.string(), nullable=True),  # TEXT: train, val, test 
        pa.field('source', pa.string(), nullable=True),       # JSONB (stored as string)
        pa.field('meta', pa.string(), nullable=True)          # JSONB (stored as string)
    ]),
    
    'annotations': pa.schema([
        pa.field('id', pa.int32(), nullable=False),           # INTEGER PRIMARY KEY
        pa.field('image_id', pa.int32(), nullable=False),     # INTEGER FOREIGN KEY
        pa.field('category_id', pa.int32(), nullable=False),  # INTEGER
        pa.field('video_id', pa.int32(), nullable=False),     # INTEGER FOREIGN KEY
        pa.field('track_id', pa.int32(), nullable=False),     # INTEGER
        pa.field('area', pa.int32(), nullable=False),         # INTEGER
        pa.field('bbox_x', pa.int32(), nullable=False),       # INTEGER
        pa.field('bbox_y', pa.int32(), nullable=False),       # INTEGER
        pa.field('bbox_width', pa.int32(), nullable=False),   # INTEGER
        pa.field('bbox_height', pa.int32(), nullable=False)   # INTEGER
    ]),
    
    'tracks': pa.schema([
        pa.field('id', pa.int32(), nullable=False),           # INTEGER PRIMARY KEY
        pa.field('category_id', pa.int32(), nullable=False),  # INTEGER
        pa.field('video_id', pa.int32(), nullable=False)      # INTEGER FOREIGN KEY
    ])
}


# Set up logging to both console and file
def setup_logging():
    """Set up logging to both console and file in logs directory."""
    # Create logs directory if it doesn't exist
    logs_dir = Path(__file__).parent.parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Create a timestamp for the log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"processing_script_{timestamp}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()


def apply_pyarrow_schema(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Apply PyArrow schema to DataFrame and ensure correct data types.
    
    Args:
        df: DataFrame to apply schema to
        table_name: Name of the table (key in PARQUET_SCHEMAS)
        
    Returns:
        DataFrame with correct column types matching PyArrow schema
    """
    schema = PARQUET_SCHEMAS[table_name]
    
    # Convert DataFrame to PyArrow Table with schema
    try:
        # First, ensure all required columns exist
        schema_fields = {field.name for field in schema}
        missing_cols = schema_fields - set(df.columns)
        if missing_cols:
            logger.error(f"Missing columns in DataFrame: {missing_cols}")
            raise ValueError(f"Missing required columns for table '{table_name}': {missing_cols}")

        # Convert to PyArrow Table with schema
        table = pa.Table.from_pandas(df, schema=schema)
        
        # Convert back to pandas DataFrame
        df_schema_applied = table.to_pandas()
        
        logger.debug(f"Applied PyArrow schema to table: {table_name}")
        return df_schema_applied
        
    except Exception as e:
        logger.error(f"Failed to apply PyArrow schema to table {table_name}: {e}")
        raise


def load_json_file(file_path: Path) -> Dict[str, Any]:
    """
    Load and parse a JSON annotation file under /data/annotations directory.
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
    df = pd.DataFrame(categories)
    df = df[['id', 'supercategory', 'name']].copy()
    df = apply_pyarrow_schema(df, 'categories')
     
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
    df = pd.DataFrame(videos)
    
    # Ensure proper column order and types
    # Handle the 'name:' key (with colon) in the data
    if 'name:' in df.columns:
        df = df.rename(columns={'name:': 'name'})
    
    required_cols = ['id', 'height', 'width', 'name']
    available_cols = [col for col in required_cols if col in df.columns]
    df = df[available_cols].copy()
    
    df = apply_pyarrow_schema(df, 'videos')
    
    output_file = output_dir / "videos.parquet"
    df.to_parquet(output_file, index=False)
    
    logger.info(f"Saved {len(df)} videos to {output_file}")


def process_images(data: Dict[str, Any], output_dir: Path, dataset_split: str) -> None:
    """
    Process images and save as partitioned parquet.
    
    Args:
        data: Parsed JSON data
        output_dir: Output directory for parquet files
        dataset_split: Dataset split ('train', 'val', 'test')
    """
    logger.info("Processing images...")
    
    images = data.get('images', [])
    df = pd.DataFrame(images)
    
    # Keep nested fields as JSON strings for storage
    if 'source' in df.columns:
        df['source'] = df['source'].apply(lambda x: json.dumps(x) if x else None)
    
    if 'meta' in df.columns:
        df['meta'] = df['meta'].apply(lambda x: json.dumps(x) if x else None)
    
    # Add local file paths (relative from root folder)
    df['file_path'] = df['file_name'].apply(lambda x: f"data/images/{dataset_split}/{x}")
    
    # Set dataset_split based on the input file
    df['dataset_split'] = dataset_split
    
    # Ensure proper column order and types
    required_cols = ['id', 'file_name', 'file_path', 'date_time', 'height', 'width', 'video_id', 'frame_index', 'dataset_split', 'source', 'meta']
    available_cols = [col for col in required_cols if col in df.columns]
    df = df[available_cols].copy()
    
    df = apply_pyarrow_schema(df, 'images')
    
    # Save as partitioned parquet by video_id
    output_file = output_dir / "images.parquet"

    for video_id, group_df in df.groupby('video_id'):
        partition_dir = output_file / f"video_id={video_id}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"video_{video_id}_images.parquet"
        group_df.to_parquet(partition_dir / filename, index=False)
    
    logger.info(f"Saved {len(df)} images to {output_file} (partitioned by video_id)")
    logger.info(f"Number of video partitions: {df['video_id'].nunique()}")


def process_annotations(data: Dict[str, Any], output_dir: Path) -> None:
    """
    Process annotations and save as partitioned parquet.
    
    Args:
        data: Parsed JSON data
        output_dir: Output directory for parquet files
    """
    logger.info("Processing annotations...")
    
    annotations = data.get('annotations', [])
    df = pd.DataFrame(annotations)
    
    # Extract bbox coordinates
    if 'bbox' in df.columns:
        bbox_df = pd.DataFrame(df['bbox'].tolist(), columns=['bbox_x', 'bbox_y', 'bbox_width', 'bbox_height'])
        df = pd.concat([df.drop('bbox', axis=1), bbox_df], axis=1)
    
    # Ensure proper column order and types
    all_required_cols = ['id', 'image_id', 'category_id', 'video_id', 'track_id', 'area', 'bbox_x', 'bbox_y', 'bbox_width', 'bbox_height']
    available_cols = [col for col in all_required_cols if col in df.columns]
    df = df[available_cols].copy()
    
    df = apply_pyarrow_schema(df, 'annotations')
    
    # Save as partitioned parquet by category_id and track_id
    output_file = output_dir / "annotations.parquet"
    partition_cols = ['category_id', 'track_id']
    
    # Create descriptive filenames by manually creating partitions
    for (category_id, track_id), group_df in df.groupby(partition_cols):
        partition_dir = output_file / f"category_id={category_id}" / f"track_id={track_id}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        filename = f"category_{category_id}_track_{track_id}_annotations.parquet"
        group_df.to_parquet(partition_dir / filename, index=False)
    
    logger.info(f"Saved {len(df)} annotations to {output_file}")
    logger.info(f"Partitioned by: {partition_cols}")
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
    df = apply_pyarrow_schema(df, 'tracks')
    
    # Save as partitioned parquet by category_id (optimized for global track fusion)
    output_file = output_dir / "tracks.parquet"
    
    # Create descriptive filenames by manually creating partitions
    for category_id, group_df in df.groupby('category_id'):
        partition_dir = output_file / f"category_id={category_id}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"category_{category_id}_tracks.parquet"
        group_df.to_parquet(partition_dir / filename, index=False)
    
    logger.info(f"Saved {len(df)} tracks to {output_file} (partitioned by category_id)")
    logger.info(f"Number of category partitions: {df['category_id'].nunique()}")


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
    if 'videos' in data and data['videos']:
        videos_df = pd.DataFrame(data['videos'])
        stats["unique_video_ids"] = videos_df['id'].nunique() if 'id' in videos_df.columns else 0
    else:
        stats["unique_video_ids"] = 0
    
    # Add category-specific stats
    if 'categories' in data and data['categories']:
        categories_df = pd.DataFrame(data['categories'])
        stats["unique_supercategories"] = categories_df['supercategory'].nunique() if 'supercategory' in categories_df.columns else 0
    else:
        stats["unique_supercategories"] = 0
    
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
        
        if len(sys.argv) > 1:
            json_filename = sys.argv[1]
            json_file = annotations_dir / json_filename
            if not json_file.exists():
                logger.error(f"File not found: {json_file}")
                return
            json_files = [json_file]
        else:
            # Process only train and validation JSON files in annotations directory
            all_json_files = list(annotations_dir.glob("*.json"))
            json_files = [f for f in all_json_files if "train" in f.name or "val" in f.name or "validation" in f.name]
            if not json_files:
                logger.error(f"No train or validation JSON files found in {annotations_dir}")
                return
            logger.info(f"Found {len(json_files)} train/validation JSON files to process")
            if len(all_json_files) > len(json_files):
                skipped_files = [f.name for f in all_json_files if f not in json_files]
                logger.info(f"Skipped {len(skipped_files)} non-train/val files: {skipped_files}")
        
        # Process each JSON file
        for json_file in json_files:
            try:
                logger.info(f"Processing {json_file.name}")
                
                # Determine dataset split from filename
                if "train" in json_file.name:
                    dataset_split = "train"
                elif "val" in json_file.name or "validation" in json_file.name:
                    dataset_split = "val"
                else:
                    dataset_split = "train"  # Default to train
                    logger.warning(f"Could not determine dataset split from filename {json_file.name}, defaulting to 'train'")
                
                logger.info(f"Detected dataset split: {dataset_split}")
                
                # Load JSON data
                data = load_json_file(json_file)
                
                process_categories(data, output_dir)
                process_videos(data, output_dir)
                process_images(data, output_dir, dataset_split)
                process_annotations(data, output_dir)
                process_tracks(data, output_dir)
                
                # Save processing statistics
                save_processing_stats(data, output_dir)
                
                # Clear memory
                del data
                gc.collect()
                
                logger.info(f"Completed processing {json_file.name}")
                
            except ValueError as e:
                logger.error(f"Critical error processing {json_file.name}: {e}")
                logger.error("Stopping processing due to missing columns")
                raise  # Re-raise to stop all processing
            except Exception as e:
                logger.error(f"Error processing {json_file.name}: {e}")
                # Continue with next file instead of raising
                continue
        
        logger.info("JSON to Parquet conversion completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        raise


if __name__ == "__main__":
    main()
