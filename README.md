# Open Food Facts Exports

This repository contains the code that performs daily exports of the Open Food Facts database. It converts the raw JSONL data into various formats optimized for research, data science, and mobile applications.

## 🎯 Overview

Open Food Facts Exports is a standalone service that:
- Downloads JSONL datasets from Open Food Facts servers
- Converts data to multiple optimized formats (Parquet, CSV, TSV)
- Pushes datasets to Hugging Face and AWS S3
- Provides specialized exports for mobile applications
- Handles incremental updates and recent changes tracking

This service reduces load on the main Open Food Facts server and provides more robust, scalable data exports.

## 📊 Available Datasets & Access Points

### Production Datasets

| Dataset | Format | Description | Access |
|---------|--------|-------------|---------|
| **Food Products** | Parquet | Complete food products database | [🤗 Hugging Face](https://huggingface.co/datasets/openfoodfacts/product-database) |
| **Beauty Products** | Parquet | Complete beauty products database | [🤗 Hugging Face](https://huggingface.co/datasets/openfoodfacts/product-database) |
| **Open Prices** | Parquet | Price data from Open Prices project | [🤗 Hugging Face](https://huggingface.co/datasets/openfoodfacts/product-database) |
| **Mobile App Export** | TSV.GZ | Optimized for mobile applications | [📱 S3 Download](https://openfoodfacts-ds.s3.eu-west-3.amazonaws.com/openfoodfacts-mobile-dump-products.tsv.gz) |
| **Recent Changes** | JSONL.GZ | Daily incremental changes | [📥 Download](https://world.openfoodfacts.org/data/openfoodfacts_recent_changes.jsonl.gz) |
| **Full CSV Export** | CSV.GZ | Complete products in CSV format | [📥 Download](https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz) |

### AWS Open Data Integration

Open Food Facts datasets are available on AWS Open Data:
- **AWS Marketplace**: [Open Food Facts Database](https://aws.amazon.com/marketplace/pp/prodview-j2ukhra3ixcgs#links)  
- **Images Dataset**: [AWS Images Documentation](https://openfoodfacts.github.io/openfoodfacts-server/api/aws-images-dataset/)
- **Blog Post**: [Open Food Facts Images on AWS Open Dataset](https://blog.openfoodfacts.org/en/news/open-food-facts-images-on-aws-open-dataset-the-ultimate-food-image-database)

### Data Formats & Use Cases

#### Parquet Format (Recommended for Data Science)
- **Optimized for**: Analytics, research, big data processing
- **Features**: Column storage, efficient compression, schema evolution
- **Tools**: Works with Pandas, DuckDB, Apache Arrow, Spark
- **Update Frequency**: Daily

#### Mobile App Export (TSV.GZ)
- **Optimized for**: Mobile applications with limited bandwidth
- **Features**: Essential fields only, highly compressed
- **Fields**: `code`, `product_name`, `quantity`, `brands`, `nutrition_grade_fr`, `nova_group`, `ecoscore_grade`
- **Size**: ~10x smaller than full dataset
- **Update Frequency**: Daily

#### Recent Changes Export (JSONL.GZ)
- **Purpose**: Track daily product updates and additions
- **Format**: Newline-delimited JSON with change metadata
- **Use Case**: Incremental updates, change tracking, real-time sync
- **Update Frequency**: Daily

## 🏗️ Architecture

The service consists of two main components:

### Scheduler Service
- Triggers daily export jobs at scheduled times
- Manages export workflows and dependencies
- Handles error reporting and monitoring

### Worker Services  
- Download JSONL datasets from Open Food Facts servers
- Process and convert data formats (JSONL → Parquet, CSV, TSV)
- Upload results to Hugging Face Hub and AWS S3
- Generate mobile-optimized exports
- Handle Open Prices data integration

```
[Scheduler] → [Redis Queue] → [Workers]
                                 ↓
                    [Hugging Face Hub] + [AWS S3]
```

## 🚀 Quick Start

### Using Docker Compose (Recommended)

```bash
# Clone the repository
git clone https://github.com/openfoodfacts/openfoodfacts-exports.git
cd openfoodfacts-exports

# Copy and configure environment
cp .env.example .env
# Edit .env with your credentials

# Start the services
make dev
```

### Manual Export Trigger

```bash
# Trigger a specific export
make cli args="launch-export off"    # Food products
make cli args="launch-export obf"    # Beauty products  
make cli args="launch-export op"     # Open Prices
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DATASET_DIR` | Local dataset storage path | `./datasets` | Yes |
| `REDIS_HOST` | Redis server hostname | `redis` | Yes |
| `ENABLE_HF_PUSH` | Enable Hugging Face uploads | `0` | No |
| `ENABLE_S3_PUSH` | Enable S3 uploads | `0` | No |
| `HF_TOKEN` | Hugging Face API token | - | For HF push |
| `AWS_ACCESS_KEY` | AWS access key | - | For S3 push |
| `AWS_SECRET_KEY` | AWS secret key | - | For S3 push |
| `AWS_S3_DATASET_BUCKET` | S3 bucket name | `openfoodfacts-ds` | For S3 push |
| `SENTRY_DSN` | Error tracking DSN | - | No |
| `ENVIRONMENT` | Environment name | `dev` | No |

### Production Setup

For production deployment:

```bash
# Create external volumes
make create_external_volumes

# Set production environment
export ENVIRONMENT=prod
export ENABLE_HF_PUSH=1
export ENABLE_S3_PUSH=1

# Start services
make up
```

## 🔍 Data Schema & Fields

### Food Products Schema
The Parquet exports include 100+ fields covering:
- **Basic Info**: `code`, `product_name`, `brands`, `quantity`  
- **Nutrition**: `nutriments`, `nutriscore_grade`, `nova_group`
- **Ingredients**: `ingredients`, `ingredients_text`, `allergens_tags`
- **Environmental**: `ecoscore_grade`, `ecoscore_score`, `packaging`
- **Quality**: `data_quality_errors_tags`, `completeness`
- **Images**: `images` with multiple sizes and metadata
- **Metadata**: `created_t`, `last_modified_t`, `countries_tags`

### Mobile App Fields (Subset)
- `code`: Product barcode
- `product_name`: Product name
- `quantity`: Package quantity  
- `brands`: Brand names
- `nutrition_grade_fr`: Nutri-Score (A-E)
- `nova_group`: NOVA processing level (1-4)
- `ecoscore_grade`: Eco-Score (A-E)

## 💡 Usage Examples

### DuckDB Analysis
```sql
-- Load Parquet file
SELECT COUNT(*) FROM 'food.parquet';

-- Analyze nutrition grades
SELECT nutriscore_grade, COUNT(*) 
FROM 'food.parquet' 
GROUP BY nutriscore_grade;

-- Find high-protein products
SELECT code, product_name->[0].text, nutriments
FROM 'food.parquet'
WHERE list_contains(
    list_transform(nutriments, x -> x.name), 
    'proteins'
);
```

### Python with Pandas
```python
import pandas as pd

# Load dataset
df = pd.read_parquet('food.parquet')

# Basic analysis
print(f"Dataset contains {len(df):,} products")
print(df.nutriscore_grade.value_counts())

# Filter for organic products
organic = df[df.labels_tags.str.contains('en:organic', na=False)]
```

### Mobile App Integration
```javascript
// Download mobile export
const response = await fetch(
  'https://openfoodfacts-ds.s3.eu-west-3.amazonaws.com/openfoodfacts-mobile-dump-products.tsv.gz'
);
// Parse TSV data for offline usage
```

## 📈 Migration Status

### ✅ Completed Exports
- [x] Parquet exports to Hugging Face
- [x] Mobile app TSV exports to S3  
- [x] Open Prices data integration
- [x] Beauty products support
- [x] Automated daily scheduling

### 🚧 Planned Migrations
- [ ] CSV exports (en, fr) from Product Opener
- [ ] RDF exports (en, fr) 
- [ ] Daily diff/delta exports
- [ ] JSONL exports migration
- [ ] MongoDB exports migration
- [ ] Open Pet Food Facts support
- [ ] Open Products Facts support

## 🐛 Troubleshooting

### Common Issues

**Export Job Fails**
- Check Redis connection: `make log`
- Verify disk space in datasets volume
- Check environment variables

**Parquet Generation Errors**  
- Review validation errors in logs
- Check for malformed JSON in source data
- Ensure adequate memory allocation (8GB+ recommended)

**S3/HuggingFace Upload Issues**
- Verify credentials are set correctly
- Check network connectivity 
- Ensure proper permissions for target buckets/repos

### Data Quality Issues
Known issues being addressed:
- JSON escaping in ingredients field ([#42](https://github.com/openfoodfacts/openfoodfacts-exports/issues/42))
- CSV quoting for special characters ([#34](https://github.com/openfoodfacts/openfoodfacts-exports/issues/34))
- DuckDB JSONL import compatibility ([#35](https://github.com/openfoodfacts/openfoodfacts-exports/issues/35))

## 🤝 Contributing

### Development Setup
1. Clone the repository
2. Copy `.env.example` to `.env` and configure
3. Run `make dev` to start development environment
4. Make changes and test with `make tests`

### Running Tests
```bash
make unit-tests        # Unit tests
make integration-tests # Integration tests  
make quality          # Full quality checks
```

### Code Quality
- Code formatting: `make lint`
- Type checking: `make mypy`  
- All checks: `make checks`

## 📞 Support & Community

- **Issues**: [GitHub Issues](https://github.com/openfoodfacts/openfoodfacts-exports/issues)
- **Discussions**: [Open Food Facts Forum](https://forum.openfoodfacts.org/)
- **Slack**: [Open Food Facts Slack](https://slack.openfoodfacts.org/)
- **Main Website**: [https://world.openfoodfacts.org/data](https://world.openfoodfacts.org/data)

## 📚 Additional Documentation

- **[Data Exports Guide](docs/DATA_EXPORTS.md)** - Detailed documentation for mobile and recent changes exports
- **[API Access Guide](docs/API_ACCESS.md)** - Comprehensive API documentation and usage examples
- **[Developer Guide](docs/DEVELOPER_GUIDE.md)** - Setup and contribution instructions for developers
- **[Troubleshooting Guide](docs/TROUBLESHOOTING.md)** - Solutions for common issues and problems

## 📄 License

This project is licensed under the GNU Affero General Public License v3.0 - see the [LICENSE](LICENSE) file for details.