# Troubleshooting Guide

This guide helps resolve common issues encountered when working with Open Food Facts Exports.

## Common Issues & Solutions

### 1. Export Job Failures

#### Symptoms
- Jobs fail with validation errors
- Incomplete Parquet files
- Workers crashing

#### Issue: Pydantic Validation Errors ([#28](https://github.com/openfoodfacts/openfoodfacts-exports/issues/28))

**Error Message:**
```
ValidationError: 1 validation error for FoodProduct
nova_groups
  Input should be a valid string [type=string_type, input_value=1, input_type=int]
```

**Root Cause:** Source data contains integer values where strings are expected.

**Solution:**
```python
# In product schema, add validator
@model_validator(mode="before")
@classmethod
def parse_nova_groups(cls, data: dict):
    nova_groups = data.get("nova_groups")
    if nova_groups and isinstance(nova_groups, int):
        data["nova_groups"] = str(nova_groups)
    return data
```

**Prevention:**
- Add robust type coercion in Pydantic models
- Monitor data quality in upstream sources
- Add integration tests with real data samples

### 2. JSON Parsing Issues

#### Issue: Malformed JSON in Ingredients Field ([#42](https://github.com/openfoodfacts/openfoodfacts-exports/issues/42))

**Error:** DuckDB fails to parse ingredients JSON due to unescaped characters

**Example Bad Data:**
```json
{"text": "mbno et dig}ycéfides diacides\ngras"}
```

**Solution:**
```python
# In FoodProduct.serialize_ingredients()
@field_serializer("ingredients")
def serialize_ingredients(self, ingredients, _info) -> str | None:
    if ingredients is None:
        return None
    try:
        # Use orjson for proper escaping
        return orjson.dumps([ing.model_dump() for ing in ingredients]).decode("utf-8")
    except Exception as e:
        logger.warning(f"Failed to serialize ingredients: {e}")
        return None
```

**Verification:**
```sql
-- Test JSON parsing in DuckDB
SELECT ingredients::json[]
FROM 'food.parquet' 
WHERE code = '0002000002404'
LIMIT 1;
```

### 3. CSV Export Issues

#### Issue: Improper CSV Quoting ([#34](https://github.com/openfoodfacts/openfoodfacts-exports/issues/34))

**Error:** Python CSV parser fails with "field larger than field limit"

**Root Cause:** Special characters (quotes, newlines) not properly escaped in CSV output.

**Solution:**
```python
# In CSV export query
MOBILE_APP_DUMP_SQL_QUERY = r"""
COPY ( 
    SELECT * FROM read_parquet('{dataset_path}')
) TO '{output_path}' (
    HEADER true, 
    DELIMITER '\t', 
    QUOTE '"',
    FORCE_QUOTE true
);
"""
```

**Verification:**
```python
import csv
with open("products.csv", "r") as f:
    reader = csv.reader(f, delimiter="\t")
    row_count = sum(1 for row in reader)
    print(f"Successfully parsed {row_count} rows")
```

### 4. DuckDB Import Issues

#### Issue: JSONL Not Recognized by DuckDB ([#35](https://github.com/openfoodfacts/openfoodfacts-exports/issues/35))

**Problem:** DuckDB shows single JSON column instead of parsed fields.

**Debugging Steps:**
```sql
-- Check file structure
SELECT * FROM read_ndjson('openfoodfacts-products.jsonl', ignore_errors=true) LIMIT 5;

-- Force column detection
SELECT * FROM read_ndjson(
    'openfoodfacts-products.jsonl', 
    auto_detect=true,
    sample_size=1000,
    ignore_errors=true
) LIMIT 5;
```

**Solution:**
- Ensure JSONL file has consistent schema in first 1000 lines
- Use `sample_size=-1` to scan entire file
- Pre-process JSONL to ensure consistent field presence

### 5. Memory Issues

#### Symptoms
- Workers killed with OOMKilled status
- Slow processing times
- System becomes unresponsive

**Solutions:**

**Increase Memory Limits:**
```yaml
# docker-compose.yml
services:
  workers:
    mem_limit: 16g  # Increase from default 8g
```

**Optimize Processing:**
```python
# Use smaller batch sizes
def convert_jsonl_to_parquet(
    # ...
    batch_size: int = 512,  # Reduce from 1024
    row_group_size: int = 61_440,  # Reduce from 122_880
):
```

**Monitor Memory Usage:**
```bash
# Check memory usage
docker stats

# Check system memory
free -h
```

### 6. Network & Download Issues

#### Issue: Dataset Download Failures

**Error Messages:**
- Connection timeouts
- SSL certificate errors
- HTTP 5xx errors

**Solutions:**

**Retry Logic:**
```python
import requests
from time import sleep

def download_with_retry(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                sleep(2 ** attempt)  # Exponential backoff
                continue
            raise
```

**Check Network Connectivity:**
```bash
# Test connectivity
curl -I https://static.openfoodfacts.org/data/

# Check DNS resolution
nslookup static.openfoodfacts.org

# Test with different timeout
curl --max-time 300 -I https://static.openfoodfacts.org/data/
```

### 7. S3 Upload Issues

#### Issue: Mobile App Export Not Available ([#45](https://github.com/openfoodfacts/openfoodfacts-exports/issues/45))

**Check Upload Status:**
```python
# Verify S3 configuration
import os
print(f"S3 Push Enabled: {os.getenv('ENABLE_S3_PUSH')}")
print(f"AWS Access Key: {os.getenv('AWS_ACCESS_KEY', 'Not set')}")
print(f"S3 Bucket: {os.getenv('AWS_S3_DATASET_BUCKET')}")
```

**Test S3 Connection:**
```python
from openfoodfacts_exports.utils import get_minio_client

client = get_minio_client()
try:
    # Test bucket access
    bucket = "openfoodfacts-ds"
    objects = client.list_objects(bucket, max_keys=1)
    print("S3 connection successful")
except Exception as e:
    print(f"S3 connection failed: {e}")
```

**Enable S3 Push:**
```bash
# In production environment
export ENABLE_S3_PUSH=1
export AWS_ACCESS_KEY=your_key
export AWS_SECRET_KEY=your_secret
```

### 8. Hugging Face Upload Issues

**Error Messages:**
- Authentication errors
- Repository not found
- Upload timeouts

**Solutions:**

**Verify Token:**
```bash
# Check HF token
huggingface-cli whoami

# Test token in environment
python3 -c "
from huggingface_hub import HfApi
api = HfApi()
print(api.whoami())
"
```

**Check Repository Access:**
```python
from huggingface_hub import HfApi

api = HfApi()
try:
    repo_info = api.repo_info(
        repo_id="openfoodfacts/product-database",
        repo_type="dataset"
    )
    print("Repository access confirmed")
except Exception as e:
    print(f"Repository access failed: {e}")
```

### 9. Redis Connection Issues

**Symptoms:**
- Jobs not being queued
- Workers not processing tasks
- Connection refused errors

**Debugging:**
```bash
# Check Redis status
docker compose ps redis

# Test Redis connection
docker compose exec redis redis-cli ping

# View Redis logs
docker compose logs redis
```

**Common Solutions:**
```bash
# Restart Redis
docker compose restart redis

# Check Redis configuration
docker compose exec redis redis-cli CONFIG GET "*"

# Clear Redis queues (if needed)
docker compose exec redis redis-cli FLUSHALL
```

### 10. Data Quality Issues

#### Issue: Missing Photographer Attribution ([#33](https://github.com/openfoodfacts/openfoodfacts-exports/issues/33))

**Problem:** Images missing uploader information for CC attribution.

**Check Data:**
```sql
-- Check uploader data availability
SELECT 
    COUNT(*) as total_images,
    COUNT(uploader) as with_uploader,
    COUNT(*) - COUNT(uploader) as missing_uploader
FROM (
    SELECT unnest(images) as img
    FROM 'food.parquet'
) t, unnest([img.uploader]) as uploader;
```

**Solution:** Ensure image processing preserves uploader metadata from source.

## Performance Optimization

### 1. Processing Speed Issues

**Symptoms:**
- Exports take >8 hours to complete
- High CPU usage
- Slow disk I/O

**Solutions:**

**Optimize DuckDB Settings:**
```python
# Increase thread count
duckdb.sql("SET threads to 8;")  # Adjust based on CPU cores

# Optimize memory usage
duckdb.sql("SET memory_limit = '12GB';")

# Disable preserving insertion order for speed
duckdb.sql("SET preserve_insertion_order = false;")
```

**Use Efficient Data Types:**
```python
# Prefer PyArrow over Pandas for large datasets
import pyarrow.parquet as pq

# Read in batches
parquet_file = pq.ParquetFile('large_dataset.parquet')
for batch in parquet_file.iter_batches(batch_size=1000):
    # Process batch
    pass
```

### 2. Disk Space Issues

**Monitor Disk Usage:**
```bash
# Check available space
df -h /app/datasets

# Check Docker volume usage
docker system df

# Find large files
du -h /app/datasets/* | sort -hr | head -10
```

**Cleanup Strategies:**
```bash
# Remove old temporary files
find /tmp -name "*.tmp" -mtime +1 -delete

# Clean up Docker artifacts
docker system prune -f

# Compress old datasets
gzip datasets/*.parquet.old
```

## Monitoring & Alerts

### Log Analysis

**Check Export Status:**
```bash
# View recent logs
docker compose logs --tail 100 -f scheduler workers

# Search for errors
docker compose logs | grep -i error

# Filter by specific job
docker compose logs | grep "export_job"
```

**Key Log Patterns:**
- `Start export job for flavor`: Job initiation
- `JSONL to Parquet conversion completed`: Successful conversion
- `Data successfully pushed to Hugging Face`: Upload success
- `ValidationError`: Data validation issues
- `ConnectionError`: Network or service issues

### Health Checks

**Service Health:**
```bash
# Check all services
make status

# Test job queue
make cli args="launch-export off --dry-run"

# Verify data freshness
ls -la datasets/
```

**Data Quality Checks:**
```python
# Verify Parquet file integrity
import pyarrow.parquet as pq

table = pq.read_table('datasets/food.parquet')
print(f"Rows: {table.num_rows:,}")
print(f"Columns: {table.num_columns}")
print(f"Size: {table.nbytes / 1024**3:.1f} GB")

# Check for null values in key fields
df = table.to_pandas()
print("Missing codes:", df.code.isnull().sum())
print("Missing names:", df.product_name.isnull().sum())
```

## Getting Help

### Before Reporting Issues

1. **Check existing issues**: Search GitHub for similar problems
2. **Verify configuration**: Double-check environment variables
3. **Review logs**: Collect relevant error messages
4. **Test isolation**: Try reproducing with minimal setup

### Issue Reporting Template

```markdown
## Problem Description
Brief description of the issue

## Environment
- OS: [Ubuntu 20.04]
- Docker version: [20.10.x]
- Service: [scheduler/workers/all]

## Steps to Reproduce
1. Step one
2. Step two
3. Step three

## Expected Behavior
What should happen

## Actual Behavior
What actually happens

## Error Logs
```
[Paste relevant log output here]
```

## Additional Context
Any other relevant information
```

### Community Resources

- **GitHub Issues**: https://github.com/openfoodfacts/openfoodfacts-exports/issues
- **Open Food Facts Slack**: https://slack.openfoodfacts.org/
- **Forum**: https://forum.openfoodfacts.org/
- **Developer Documentation**: https://wiki.openfoodfacts.org/

### Emergency Contacts

For production issues affecting data availability:
- Create GitHub issue with `P0` label
- Post in #dev-exports Slack channel
- Email: contact@openfoodfacts.org