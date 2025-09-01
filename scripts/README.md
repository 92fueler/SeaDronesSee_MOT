# SeaDronesSee MOT Project Scripts

This directory contains utility scripts for managing the SeaDronesSee MOT project.

## Available Scripts

### 1. `setup.sh` - Project Setup
Sets up the virtual environment and installs dependencies using uv.

**Usage:**
```bash
# Interactive setup (prompts for cleanup)
./scripts/setup.sh

# Clean setup (removes old environment without prompting)
./scripts/setup.sh --clean

# Force recreation of virtual environment
./scripts/setup.sh --force

# Show help
./scripts/setup.sh --help
```

**What it does:**
- Checks for uv installation
- Cleans up old virtual environment (optional)
- Creates new virtual environment using uv
- Installs dependencies from pyproject.toml
- Verifies installation by testing key packages

### 2. `cleanup.sh` - Project Cleanup
Cleans up various project files and directories.

**Usage:**
```bash
# Clean everything
./scripts/cleanup.sh --all

# Clean only virtual environment
./scripts/cleanup.sh --env

# Clean only cache files
./scripts/cleanup.sh --cache

# Clean only log files
./scripts/cleanup.sh --logs

# Clean only parquet files
./scripts/cleanup.sh --parquet

# Show help
./scripts/cleanup.sh --help
```

**What it cleans:**
- Virtual environment (`.venv`)
- Lock files (`uv.lock`)
- Cache files (`__pycache__`, `.pyc`)
- Log files (`logs/`)
- Parquet files (`data/parquet/`)

### 3. `convert_json_to_parquet.sh` - Data Conversion
Converts JSON annotation files to parquet format with optimized partitioning.

**Usage:**
```bash
# Convert training data
./scripts/convert_json_to_parquet.sh

# Clean and convert
./scripts/convert_json_to_parquet.sh --clean

# Show help
./scripts/convert_json_to_parquet.sh --help
```

**What it does:**
- Processes `instances_train_objects_in_water.json`
- Creates parquet files with descriptive filenames
- Optimizes partitioning for re-ID and cross-video association
- Generates statistics and summary

## Quick Start

1. **Setup the project:**
   ```bash
   ./scripts/setup.sh --clean
   ```

2. **Convert data:**
   ```bash
   ./scripts/convert_json_to_parquet.sh --clean
   ```

3. **Clean up when done:**
   ```bash
   ./scripts/cleanup.sh --all
   ```

## Script Features

- **Colored Output**: Blue info, green success, yellow warnings, red errors
- **Error Handling**: Exits on errors with helpful messages
- **Interactive Prompts**: Asks before removing important files
- **Dependency Checks**: Validates required tools (uv)
- **Comprehensive Logging**: Detailed progress information

## Requirements

- **uv**: Python package manager (https://docs.astral.sh/uv/)
- **bash**: Unix shell
- **Python 3.10+**: For the project dependencies

## File Structure After Setup

```
SeaDronesSee_MOT/
├── .venv/                    # Virtual environment
├── data/
│   ├── annotations/          # JSON annotation files
│   └── parquet/             # Converted parquet files
├── logs/                    # Log files
├── scripts/                 # This directory
└── src/                     # Source code
```
