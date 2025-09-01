#!/bin/bash

# SeaDronesSee MOT Project Cleanup Script
# This script cleans up virtual environment and cache files

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

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --all       Clean everything (virtual env, cache, logs, parquet)"
    echo "  --env       Clean only virtual environment and lock files"
    echo "  --cache     Clean only cache files (__pycache__, .pyc)"
    echo "  --logs      Clean only log files"
    echo "  --parquet   Clean only parquet files"
    echo "  --help, -h  Show this help message"
    echo ""
    echo "This script cleans up the SeaDronesSee MOT project:"
    echo "- Virtual environment (.venv)"
    echo "- Lock files (uv.lock)"
    echo "- Cache files (__pycache__, .pyc)"
    echo "- Log files (logs/)"
    echo "- Parquet files (data/parquet/)"
}

# Function to clean virtual environment
clean_env() {
    print_status "Cleaning virtual environment..."
    
    if [ -d "$PROJECT_ROOT/.venv" ]; then
        rm -rf "$PROJECT_ROOT/.venv"
        print_success "Virtual environment removed"
    else
        print_status "No virtual environment found"
    fi
    
    if [ -f "$PROJECT_ROOT/uv.lock" ]; then
        rm -f "$PROJECT_ROOT/uv.lock"
        print_success "uv.lock removed"
    else
        print_status "No uv.lock found"
    fi
}

# Function to clean cache files
clean_cache() {
    print_status "Cleaning cache files..."
    
    # Remove __pycache__ directories
    if find "$PROJECT_ROOT" -name "__pycache__" -type d | grep -q .; then
        find "$PROJECT_ROOT" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
        print_success "__pycache__ directories removed"
    else
        print_status "No __pycache__ directories found"
    fi
    
    # Remove .pyc files
    if find "$PROJECT_ROOT" -name "*.pyc" -type f | grep -q .; then
        find "$PROJECT_ROOT" -name "*.pyc" -type f -delete
        print_success ".pyc files removed"
    else
        print_status "No .pyc files found"
    fi
}

# Function to clean log files
clean_logs() {
    print_status "Cleaning log files..."
    
    if [ -d "$PROJECT_ROOT/logs" ]; then
        rm -rf "$PROJECT_ROOT/logs"/*
        print_success "Log files removed"
    else
        print_status "No logs directory found"
    fi
}

# Function to clean parquet files
clean_parquet() {
    print_status "Cleaning parquet files..."
    
    if [ -d "$PROJECT_ROOT/data/parquet" ]; then
        rm -rf "$PROJECT_ROOT/data/parquet"/*
        print_success "Parquet files removed"
    else
        print_status "No parquet directory found"
    fi
}

# Parse command line arguments
CLEAN_ALL=false
CLEAN_ENV=false
CLEAN_CACHE=false
CLEAN_LOGS=false
CLEAN_PARQUET=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --all)
            CLEAN_ALL=true
            shift
            ;;
        --env)
            CLEAN_ENV=true
            shift
            ;;
        --cache)
            CLEAN_CACHE=true
            shift
            ;;
        --logs)
            CLEAN_LOGS=true
            shift
            ;;
        --parquet)
            CLEAN_PARQUET=true
            shift
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# If no specific option is provided, show usage
if [ "$CLEAN_ALL" = false ] && [ "$CLEAN_ENV" = false ] && [ "$CLEAN_CACHE" = false ] && [ "$CLEAN_LOGS" = false ] && [ "$CLEAN_PARQUET" = false ]; then
    show_usage
    exit 0
fi

# Main cleanup process
main() {
    print_status "Starting cleanup process..."
    
    if [ "$CLEAN_ALL" = true ]; then
        print_warning "Cleaning everything..."
        clean_env
        clean_cache
        clean_logs
        clean_parquet
    else
        if [ "$CLEAN_ENV" = true ]; then
            clean_env
        fi
        if [ "$CLEAN_CACHE" = true ]; then
            clean_cache
        fi
        if [ "$CLEAN_LOGS" = true ]; then
            clean_logs
        fi
        if [ "$CLEAN_PARQUET" = true ]; then
            clean_parquet
        fi
    fi
    
    print_success "Cleanup completed successfully!"
}

# Run main function
main
