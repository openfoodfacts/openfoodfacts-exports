# Developer Guide

This guide provides comprehensive information for developers who want to contribute to the Open Food Facts Exports project.

## Development Environment Setup

### Prerequisites
- Docker & Docker Compose
- Python 3.10+ (for local development)
- Git
- Make

### Quick Start
```bash
# Clone the repository
git clone https://github.com/openfoodfacts/openfoodfacts-exports.git
cd openfoodfacts-exports

# Set up environment
cp .env.example .env
# Edit .env with your configuration

# Start development environment
make dev
```

### Environment Configuration

#### Required Environment Variables
```bash
# .env file
DATASET_DIR=/app/datasets
REDIS_HOST=redis

# Optional for testing integrations
ENABLE_HF_PUSH=0
ENABLE_S3_PUSH=0
HF_TOKEN=your_hugging_face_token
AWS_ACCESS_KEY=your_aws_access_key
AWS_SECRET_KEY=your_aws_secret_key
AWS_S3_DATASET_BUCKET=openfoodfacts-ds

# Monitoring (optional)
SENTRY_DSN=your_sentry_dsn
ENVIRONMENT=dev
```

## Project Architecture

### Service Components

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Scheduler  │    │    Redis    │    │   Workers   │
│             │───▶│    Queue    │───▶│             │
│ (APScheduler)│    │             │    │ (RQ Workers)│
└─────────────┘    └─────────────┘    └─────────────┘
                                             │
                                             ▼
                   ┌─────────────────────────────────────────┐
                   │            Export Outputs               │
                   │  ┌─────────────┐  ┌─────────────────────┐│
                   │  │ Hugging Face│  │      AWS S3         ││
                   │  │    Hub      │  │                     ││
                   │  │ (Parquet)   │  │ (Mobile TSV/CSV)    ││
                   │  └─────────────┘  └─────────────────────┘│
                   └─────────────────────────────────────────┘
```

### Directory Structure
```
openfoodfacts_exports/
├── __init__.py
├── main.py              # CLI entry points
├── settings.py          # Configuration
├── tasks.py             # Job orchestration
├── scheduler.py         # Daily scheduling
├── utils.py             # Utilities (S3, Sentry)
├── types.py             # Type definitions
├── exports/             # Export implementations
│   ├── csv.py          # Mobile TSV export
│   └── parquet/        # Parquet exports
│       ├── __init__.py # Main conversion logic
│       ├── common.py   # Shared schemas
│       ├── food.py     # Food product schema
│       ├── beauty.py   # Beauty product schema
│       └── price.py    # Open Prices schema
└── workers/            # RQ worker configuration
    ├── main.py
    ├── queues.py
    └── redis.py
```

## Code Organization

### Export Types
The system supports multiple export "flavors" defined in `types.py`:

```python
class ExportFlavor(str, enum.Enum):
    off = "off"     # Open Food Facts (food)
    obf = "obf"     # Open Beauty Facts  
    opf = "opf"     # Open Products Facts
    opff = "opff"   # Open Pet Food Facts
    op = "op"       # Open Prices
```

### Data Processing Pipeline

1. **Download**: JSONL files from Open Food Facts servers
2. **Transform**: Convert to structured Pydantic models
3. **Export**: Generate Parquet/CSV/TSV formats
4. **Upload**: Push to Hugging Face Hub and/or AWS S3

### Pydantic Schemas
All product data is validated using Pydantic models:

```python
class FoodProduct(Product):
    # Food-specific fields
    nutriscore_grade: str | None = None
    nova_group: int | None = None
    ingredients: list[Ingredient] | None = None
    # ... 100+ more fields
```

## Development Workflow

### Running Services Locally

#### Start All Services
```bash
make dev  # Starts scheduler, workers, and Redis
```

#### Individual Services
```bash
# Scheduler only
make up service=scheduler

# Workers only  
make up service=workers

# Redis only
make up service=redis
```

### Testing Exports

#### Trigger Manual Export
```bash
# Food products export
make cli args="launch-export off"

# Beauty products export  
make cli args="launch-export obf"

# Open Prices export
make cli args="launch-export op"
```

#### Monitor Jobs
```bash
# View logs
make log

# Check service status
make status
```

### Code Quality

#### Linting & Formatting
```bash
make lint          # Auto-fix code style
make checks        # Run all quality checks
```

#### Type Checking
```bash
make mypy          # Static type analysis
```

#### Testing
```bash
make unit-tests         # Fast unit tests
make integration-tests  # Full integration tests
make tests             # All tests
```

## Adding New Export Types

### 1. Define Product Schema

Create a new schema in `exports/parquet/`:

```python
# exports/parquet/petfood.py
from .common import Product

class PetFoodProduct(Product):
    # Pet food specific fields
    pet_type: str | None = None
    life_stage: str | None = None
    # ... other fields
    
# PyArrow schema definition
PETFOOD_PRODUCT_SCHEMA = pa.schema([
    pa.field("pet_type", pa.string(), nullable=True),
    pa.field("life_stage", pa.string(), nullable=True),
    # ... other fields
])
```

### 2. Register Export Type

Add to `types.py`:
```python
class ExportFlavor(str, enum.Enum):
    # ... existing types
    opff = "opff"  # Open Pet Food Facts
```

### 3. Update Export Logic

Modify `exports/parquet/__init__.py`:
```python
def export_parquet(dataset_path, output_path, flavor, use_tqdm=False):
    # ... existing code
    elif flavor == Flavor.opff:
        pydantic_cls = PetFoodProduct
        schema = PETFOOD_PRODUCT_SCHEMA
        dtype_map = PETFOOD_DTYPE_MAP
```

### 4. Add Task Configuration

Update `tasks.py` to handle the new flavor:
```python
def export_job(flavor: ExportFlavor) -> None:
    # ... existing code
    if flavor == ExportFlavor.opff:
        # Handle pet food export logic
        pass
```

## Testing Strategy

### Unit Tests
Test individual functions and classes:
```python
# tests/unit/exports/test_parquet.py
def test_food_product_validation():
    """Test that FoodProduct correctly validates data"""
    data = {"code": "123", "nutriscore_grade": "A"}
    product = FoodProduct(**data)
    assert product.code == "123"
    assert product.nutriscore_grade == "A"
```

### Integration Tests
Test the complete export pipeline:
```python
# tests/integration/exports/test_parquet.py
def test_full_export_pipeline(tmp_path):
    """Test complete JSONL to Parquet conversion"""
    # Create test JSONL file
    # Run export_parquet()
    # Verify output file and content
```

### Test Data
Use small datasets for testing:
```python
# tests/data/sample_products.jsonl
{"code": "123", "product_name": "Test Product"}
{"code": "456", "product_name": "Another Product"}
```

## Debugging

### Common Development Issues

#### 1. Redis Connection Errors
```bash
# Check if Redis is running
make status

# View Redis logs
docker compose logs redis

# Restart Redis
make restart service=redis
```

#### 2. Out of Memory Errors
```bash
# Increase memory limits in docker-compose.yml
services:
  workers:
    mem_limit: 16g  # Increase from 8g
```

#### 3. Dataset Download Failures
```bash
# Check network connectivity
curl -I https://static.openfoodfacts.org/data/

# Verify credentials
env | grep AWS
env | grep HF_TOKEN
```

### Debugging Tools

#### 1. Interactive Container Access
```bash
# Access running scheduler
docker compose exec scheduler bash

# Access worker container
docker compose exec workers bash

# Run Python REPL with imports
docker compose exec workers python3 -c "
from openfoodfacts_exports.exports.parquet import export_parquet
# ... debug code here
"
```

#### 2. Local Development
```bash
# Install dependencies locally
pip install -e .

# Run specific functions
python3 -c "
from openfoodfacts_exports.tasks import export_job
export_job('off')
"
```

#### 3. Data Inspection
```bash
# Check Parquet file schema
python3 -c "
import pyarrow.parquet as pq
table = pq.read_table('datasets/food.parquet')
print(table.schema)
"

# Sample data
python3 -c "
import pandas as pd
df = pd.read_parquet('datasets/food.parquet')
print(df.head())
print(df.info())
"
```

## Performance Optimization

### Memory Management
- Use streaming/batching for large datasets
- Monitor memory usage with `docker stats`
- Adjust batch sizes in conversion functions

### Processing Speed
- Parallelize independent operations
- Use efficient data types (PyArrow vs Pandas)
- Optimize DuckDB queries

### Storage Efficiency
- Use appropriate compression levels
- Remove unnecessary fields from exports
- Regular cleanup of temporary files

## Deployment

### Production Environment
```bash
# Create production volumes
make create_external_volumes

# Set production variables
export ENVIRONMENT=prod
export ENABLE_HF_PUSH=1
export ENABLE_S3_PUSH=1

# Deploy
docker compose -f docker-compose.prod.yml up -d
```

### Monitoring
- **Logs**: Centralized logging with Sentry
- **Metrics**: Container metrics with Prometheus
- **Alerts**: Failed job notifications

### Backup Strategy
- Dataset files backed up to S3
- Redis persistence enabled
- Configuration stored in version control

## Contributing Guidelines

### Code Style
- Follow PEP 8
- Use type hints
- Write docstrings for public functions
- Keep line length ≤ 88 characters

### Git Workflow
1. Fork the repository
2. Create feature branch: `git checkout -b feature/new-export`
3. Make changes and add tests
4. Run quality checks: `make quality`
5. Commit with descriptive messages
6. Push and create pull request

### Pull Request Process
1. Ensure all tests pass
2. Update documentation if needed
3. Add entry to CHANGELOG.md
4. Request review from maintainers

### Issue Reporting
- Use GitHub issue templates
- Include error logs and reproduction steps
- Tag with appropriate labels

## Resources

### Documentation
- **Pydantic**: https://docs.pydantic.dev/
- **PyArrow**: https://arrow.apache.org/docs/python/
- **DuckDB**: https://duckdb.org/docs/
- **RQ**: https://python-rq.org/

### Open Food Facts
- **Main Site**: https://world.openfoodfacts.org
- **Developer Docs**: https://wiki.openfoodfacts.org/
- **API Docs**: https://wiki.openfoodfacts.org/API

### Community
- **Slack**: https://slack.openfoodfacts.org/
- **Forum**: https://forum.openfoodfacts.org/
- **GitHub**: https://github.com/openfoodfacts/