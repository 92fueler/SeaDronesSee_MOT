#!/bin/bash

# SeaDronesSee MOT JSON to Parquet Conversion Script
# This script converts JSON annotation files to parquet format with optimized partitioning

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Get the project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

print_status "Project root: $PROJECT_ROOT"

# Check if uv is available
if ! command -v uv &> /dev/null; then
    print_error "uv is not installed or not in PATH"
    print_status "Please install uv: https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
fi

# Check if the processing script exists
PROCESSING_SCRIPT="$PROJECT_ROOT/src/convert_to_parquet/processing_script.py"
if [ ! -f "$PROCESSING_SCRIPT" ]; then
    print_error "Processing script not found: $PROCESSING_SCRIPT"
    exit 1
fi

# Check if input JSON file exists
INPUT_FILE="$PROJECT_ROOT/data/annotations/instances_train_objects_in_water.json"
if [ ! -f "$INPUT_FILE" ]; then
    print_error "Input JSON file not found: $INPUT_FILE"
    exit 1
fi

print_status "Input file: $INPUT_FILE"

# Create output directory if it doesn't exist
OUTPUT_DIR="$PROJECT_ROOT/data/parquet"
mkdir -p "$OUTPUT_DIR"

print_status "Output directory: $OUTPUT_DIR"

# Function to clean up output directory
clean_output() {
    if [ "$1" = "clean" ]; then
        print_warning "Cleaning output directory..."
        rm -rf "$OUTPUT_DIR"/*
        print_success "Output directory cleaned"
    fi
}

# Parse command line arguments
CLEAN_OUTPUT=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN_OUTPUT=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --clean    Clean output directory before processing"
            echo "  --help, -h Show this help message"
            echo ""
            echo "This script converts SeaDronesSee MOT JSON files to parquet format"
            echo "with optimized partitioning for re-ID and cross-video association."
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Clean output if requested
if [ "$CLEAN_OUTPUT" = true ]; then
    clean_output "clean"
fi

# Change to project root directory
cd "$PROJECT_ROOT"

print_status "Starting JSON to Parquet conversion..."

# Run the processing script using uv
if uv run python "$PROCESSING_SCRIPT"; then
    print_success "JSON to Parquet conversion completed successfully!"
    
    # Show output summary
    print_status "Generated files:"
    if [ -d "$OUTPUT_DIR" ]; then
        find "$OUTPUT_DIR" -name "*.parquet" -type f | while read -r file; do
            size=$(du -h "$file" | cut -f1)
            echo "  - $(basename "$file") ($size)"
        done
        
        # Show partition counts
        if [ -d "$OUTPUT_DIR/annotations.parquet" ]; then
            category_count=$(find "$OUTPUT_DIR/annotations.parquet" -maxdepth 1 -type d | wc -l)
            track_count=$(find "$OUTPUT_DIR/annotations.parquet" -type d | wc -l)
            print_status "Annotations partitions: $category_count categories, $track_count total partitions"
        fi
        
        if [ -d "$OUTPUT_DIR/images.parquet" ]; then
            video_count=$(find "$OUTPUT_DIR/images.parquet" -maxdepth 1 -type d | wc -l)
            print_status "Images partitions: $video_count video partitions"
        fi
    fi
else
    print_error "JSON to Parquet conversion failed!"
    exit 1
fi

print_success "Script completed successfully!"
