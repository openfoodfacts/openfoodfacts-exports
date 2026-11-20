# Open Food Facts exports

This repository contains the code that performs daily exports of the Open Food Facts database.

It fetches the JSON dumps and generate and pushes the Parquet export to [Hugging Face](https://huggingface.co/datasets/openfoodfacts/product-database).

We want to move all exports from the main off server to this new service to reduce load on the main server and to make exports more robust.

## Recent changes export

The [recent changes export](https://world.openfoodfacts.org/data/openfoodfacts_recent_changes.jsonl.gz)
is a gzip-compressed JSON Lines file published by Open Food Facts. Each line
describes a product revision rather than a complete product. Use the
[full database export](https://world.openfoodfacts.org/data) if you need a
snapshot of current products.

| Field | Meaning |
| --- | --- |
| `code` | Product barcode as a string. Keep it as text to preserve leading zeroes. |
| `rev` | Product revision number. |
| `t` | Edit timestamp in Unix seconds. |
| `diffs` | Summary details about what changed. |
| `comment` | Comment associated with the revision. |
| `userid` | Contributor identifier, which can be null. |

The compressed file is large. This example streams the first five records
without downloading the entire archive or loading it into memory:

```python
import gzip
import json
from itertools import islice
from urllib.request import urlopen

url = "https://world.openfoodfacts.org/data/openfoodfacts_recent_changes.jsonl.gz"
with urlopen(url, timeout=30) as response, gzip.open(
    response, "rt", encoding="utf-8"
) as changes:
    for line in islice(changes, 5):
        change = json.loads(line)
        print(change["code"], change["rev"], change["t"])
```

## Architecture

Two services are currently running:

- a scheduler, whose only role is to trigger the export service every day
- rq workers, that take care of all tasks: JSONL file download, Parquet export,...

The following exports are yet to migrate:

- CSV (en, fr)
- RDF (en, fr)
- daily diffs

We're also considering to move JSONL and MongoDB exports from Product Opener to this service, so that Product Opener does not handle exports anymore.