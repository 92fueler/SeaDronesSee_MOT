# SeaDronesSee MOT Project


## 🚀 Quick Start

### 1. Setup the Project
```bash
# Interactive setup (prompts for cleanup)
./scripts/setup.sh

# Clean setup (removes old environment without prompting)
./scripts/setup.sh --clean

# Force recreation of virtual environment
./scripts/setup.sh --force
```

### 2. Convert Data
```bash
# Convert training data to parquet
./scripts/convert_json_to_parquet.sh

# Clean and convert
./scripts/convert_json_to_parquet.sh --clean
```

### 3. Clean Up (Optional)
```bash
# Clean everything
./scripts/cleanup.sh --all

# Clean specific components
./scripts/cleanup.sh --env      # Virtual environment
./scripts/cleanup.sh --cache    # Cache files
./scripts/cleanup.sh --logs     # Log files
./scripts/cleanup.sh --parquet  # Parquet files
```
