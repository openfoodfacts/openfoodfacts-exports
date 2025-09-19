# API & Data Access Guide

This document provides comprehensive information about accessing Open Food Facts data through various APIs and endpoints.

## Data Access Overview

Open Food Facts provides multiple ways to access its data, optimized for different use cases:

| Method | Best For | Latency | Data Freshness |
|--------|----------|---------|----------------|
| **Bulk Downloads** | Research, analytics | Low | Daily |
| **REST API** | Real-time lookups | Medium | Real-time |
| **Streaming** | Real-time monitoring | High | Live |

## Bulk Data Downloads

### 1. Parquet Files (Recommended for Analytics)

#### Hugging Face Hub (Primary)
```
https://huggingface.co/datasets/openfoodfacts/product-database
```

**Access Methods:**
```python
# Using Hugging Face datasets
from datasets import load_dataset
dataset = load_dataset("openfoodfacts/product-database")

# Using Pandas with PyArrow
import pandas as pd
df = pd.read_parquet("hf://datasets/openfoodfacts/product-database/food.parquet")

# Direct download
import requests
url = "https://huggingface.co/datasets/openfoodfacts/product-database/resolve/main/food.parquet"
```

#### Available Files
- `food.parquet` - Food products (~2GB, 2M+ products)
- `beauty.parquet` - Beauty products (~100MB, 100K+ products)  
- `price.parquet` - Open Prices data (~50MB, 1M+ prices)

### 2. Mobile Optimized Export

#### Direct Download
```
https://openfoodfacts-ds.s3.eu-west-3.amazonaws.com/openfoodfacts-mobile-dump-products.tsv.gz
```

**Specifications:**
- Format: Tab-separated values (TSV)
- Compression: gzip
- Size: ~200MB uncompressed
- Fields: 7 essential fields only
- Update: Daily at 02:00 UTC

**Usage:**
```bash
# Download and preview
curl -o mobile_dump.tsv.gz \
  "https://openfoodfacts-ds.s3.eu-west-3.amazonaws.com/openfoodfacts-mobile-dump-products.tsv.gz"

# Quick stats
zcat mobile_dump.tsv.gz | wc -l
zcat mobile_dump.tsv.gz | head -5
```

### 3. Recent Changes Export

#### Incremental Updates
```
https://world.openfoodfacts.org/data/openfoodfacts_recent_changes.jsonl.gz
```

**Use Case:** Track daily changes without downloading full dataset

```python
import gzip
import json
import requests

def get_recent_changes():
    url = "https://world.openfoodfacts.org/data/openfoodfacts_recent_changes.jsonl.gz"
    response = requests.get(url)
    
    changes = []
    with gzip.open(io.BytesIO(response.content), 'rt') as f:
        for line in f:
            changes.append(json.loads(line.strip()))
    
    return changes
```

### 4. Complete CSV Export

#### Traditional Format
```
https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz
```

**Specifications:**
- Format: CSV with tab separators
- Compression: gzip  
- Size: ~4GB uncompressed
- Fields: 180+ fields
- Update: Daily

## Real-time APIs

For real-time API access including product lookup, search, and autocomplete functionality, please refer to the official documentation:

**[Open Food Facts API Documentation](https://openfoodfacts.github.io/openfoodfacts-server/api/)**

## AWS Open Data Integration

### 1. Image Dataset

#### Registry Information
```
Registry: AWS Open Data
ARN: arn:aws:s3:::off-s3-open-data/images
Region: us-east-1
```

**Documentation:** https://openfoodfacts.github.io/openfoodfacts-server/api/aws-images-dataset/

#### Access Methods
```bash
# AWS CLI
aws s3 ls s3://off-s3-open-data/images/ --no-sign-request

# Direct HTTP access
https://off-s3-open-data.s3.amazonaws.com/images/{image_path}
```

### 2. Data Marketplace

#### AWS Data Exchange
- **Product**: Open Food Facts Database  
- **URL**: https://aws.amazon.com/marketplace/pp/prodview-j2ukhra3ixcgs
- **Format**: Multiple formats available
- **Update**: Regular updates


## Rate Limits & Best Practices

### API Rate Limits
- **Product API**: 100 requests/minute
- **Search API**: 10 requests/minute  
- **Bulk Downloads**: No specific limits, but be respectful

### Best Practices

#### 1. Caching Strategy
```python
import requests
import time
from functools import lru_cache

@lru_cache(maxsize=1000)
def get_product_cached(barcode):
    """Cache API responses to reduce requests"""
    url = f"https://world.openfoodfacts.org/api/v2/product/{barcode}"
    response = requests.get(url)
    return response.json()
```

#### 2. Bulk Processing
```python
# DON'T: Loop through API for many products
for barcode in large_list:
    product = get_product_api(barcode)  # Too many requests!

# DO: Use bulk download + local lookup
df = pd.read_parquet("food.parquet")
products = df[df.code.isin(large_list)]
```

#### 3. Error Handling
```python
import requests
from time import sleep

def get_product_robust(barcode, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(
                f"https://world.openfoodfacts.org/api/v2/product/{barcode}",
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                sleep(2 ** attempt)  # Exponential backoff
                continue
            raise
```

## Data Freshness & Update Schedules

| Data Source | Update Frequency | Typical Delay |
|-------------|-----------------|---------------|
| **Real-time API** | Immediate | 0-5 minutes |
| **Parquet exports** | Daily | 2-6 hours |
| **Mobile TSV** | Daily | 2-6 hours |
| **Recent Changes** | Daily | 1-2 hours |
| **CSV exports** | Daily | 4-8 hours |
| **AWS datasets** | Weekly/Monthly | 1-7 days |

### Update Times (UTC)
- **API**: Continuous
- **Bulk exports**: Start ~02:00 UTC, complete by ~08:00 UTC
- **Recent changes**: Updated ~01:00 UTC

## Authentication & API Keys

### Public Access
- Most endpoints are **open and free**
- No API key required for read access
- Rate limits apply

### Write Access
- Product editing requires account
- API write access needs authentication
- Contact Open Food Facts for bulk write access

## Support & Community

### Technical Support
- **GitHub Issues**: https://github.com/openfoodfacts/openfoodfacts-server/issues
- **Forum**: https://forum.openfoodfacts.org/
- **Slack**: https://slack.openfoodfacts.org/

### Documentation
- **Open Food Facts API Docs**: https://openfoodfacts.github.io/openfoodfacts-server/api/
- **AWS Docs**: https://openfoodfacts.github.io/openfoodfacts-server/api/
- **Data Fields**: https://world.openfoodfacts.org/data/data-fields.txt