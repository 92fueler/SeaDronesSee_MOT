#!/bin/bash

# SeaDronesSee MOT Project Setup Script
# This script sets up the virtual environment and dependencies using uv

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

# Function to check if uv is installed
check_uv() {
    if ! command -v uv &> /dev/null; then
        print_error "uv is not installed or not in PATH"
        print_status "Please install uv: https://docs.astral.sh/uv/getting-started/installation/"
        exit 1
    fi
    print_success "uv is available: $(uv --version)"
}

# Function to clean up old virtual environment
cleanup_old_env() {
    print_status "Checking for existing virtual environment..."
    
    # Check for uv virtual environment
    if [ -d "$PROJECT_ROOT/.venv" ]; then
        print_warning "Found existing virtual environment at .venv"
        read -p "Do you want to remove it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            print_status "Removing old virtual environment..."
            rm -rf "$PROJECT_ROOT/.venv"
            print_success "Old virtual environment removed"
        else
            print_status "Keeping existing virtual environment"
        fi
    fi
    
    # Check for other virtual environment files
    if [ -f "$PROJECT_ROOT/uv.lock" ]; then
        print_warning "Found existing uv.lock file"
        read -p "Do you want to remove it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            print_status "Removing uv.lock..."
            rm -f "$PROJECT_ROOT/uv.lock"
            print_success "uv.lock removed"
        fi
    fi
    
    # Check for __pycache__ directories
    if find "$PROJECT_ROOT" -name "__pycache__" -type d | grep -q .; then
        print_warning "Found __pycache__ directories"
        read -p "Do you want to remove them? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            print_status "Removing __pycache__ directories..."
            find "$PROJECT_ROOT" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
            print_success "__pycache__ directories removed"
        fi
    fi
}

# Function to create virtual environment
create_venv() {
    print_status "Creating virtual environment..."
    
    cd "$PROJECT_ROOT"
    
    # Create virtual environment using uv
    if uv venv; then
        print_success "Virtual environment created successfully"
    else
        print_error "Failed to create virtual environment"
        exit 1
    fi
}

# Function to install dependencies
install_dependencies() {
    print_status "Installing dependencies..."
    
    cd "$PROJECT_ROOT"
    
    # Install dependencies from pyproject.toml
    if uv pip install -e .; then
        print_success "Dependencies installed successfully"
    else
        print_error "Failed to install dependencies"
        exit 1
    fi
}

# Function to verify installation
verify_installation() {
    print_status "Verifying installation..."
    
    cd "$PROJECT_ROOT"
    
    # Check if virtual environment is activated
    if [ -z "$VIRTUAL_ENV" ]; then
        print_warning "Virtual environment not activated"
        print_status "To activate: source .venv/bin/activate"
    else
        print_success "Virtual environment is active: $VIRTUAL_ENV"
    fi
    
    # Test key dependencies
    print_status "Testing key dependencies..."
    
    # Test pandas
    if uv run python -c "import pandas; print(f'pandas {pandas.__version__}')" 2>/dev/null; then
        print_success "pandas is working"
    else
        print_error "pandas test failed"
    fi
    
    # Test pyarrow
    if uv run python -c "import pyarrow; print(f'pyarrow {pyarrow.__version__}')" 2>/dev/null; then
        print_success "pyarrow is working"
    else
        print_error "pyarrow test failed"
    fi
    
    # Test torch
    if uv run python -c "import torch; print(f'torch {torch.__version__}')" 2>/dev/null; then
        print_success "torch is working"
    else
        print_error "torch test failed"
    fi
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --clean     Clean up old environment without prompting"
    echo "  --force     Force recreation of virtual environment"
    echo "  --help, -h  Show this help message"
    echo ""
    echo "This script sets up the SeaDronesSee MOT project environment:"
    echo "1. Checks for uv installation"
    echo "2. Cleans up old virtual environment (optional)"
    echo "3. Creates new virtual environment"
    echo "4. Installs dependencies"
    echo "5. Verifies installation"
}

# Parse command line arguments
CLEAN_MODE=false
FORCE_MODE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN_MODE=true
            shift
            ;;
        --force)
            FORCE_MODE=true
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

# Main setup process
main() {
    print_status "Starting SeaDronesSee MOT project setup..."
    
    # Check uv installation
    check_uv
    
    # Clean up old environment
    if [ "$CLEAN_MODE" = true ]; then
        print_status "Cleaning up old environment..."
        rm -rf "$PROJECT_ROOT/.venv"
        rm -f "$PROJECT_ROOT/uv.lock"
        find "$PROJECT_ROOT" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
        print_success "Cleanup completed"
    elif [ "$FORCE_MODE" = true ]; then
        print_status "Force mode: cleaning up old environment..."
        rm -rf "$PROJECT_ROOT/.venv"
        rm -f "$PROJECT_ROOT/uv.lock"
        find "$PROJECT_ROOT" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
        print_success "Cleanup completed"
    else
        cleanup_old_env
    fi
    
    # Create virtual environment
    create_venv
    
    # Install dependencies
    install_dependencies
    
    # Verify installation
    verify_installation
    
    print_success "Setup completed successfully!"
    echo ""
    print_status "Next steps:"
    echo "  1. Activate virtual environment: source .venv/bin/activate"
    echo "  2. Run conversion script: ./scripts/convert_json_to_parquet.sh"
    echo "  3. Start development!"
}

# Run main function
main
