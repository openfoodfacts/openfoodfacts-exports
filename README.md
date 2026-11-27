# Open Food Facts exports

This repository contains the code that performs daily exports of the Open Food Facts database.

It fetches the JSON dumps and generate and pushes the Parquet export to [Hugging Face](https://huggingface.co/datasets/openfoodfacts/product-database).

We want to move all exports from the main off server to this new service to reduce load on the main server and to make exports more robust.

## Architecture

Two services are currently running:

- a scheduler, whose only role is to trigger the export service every day
- rq workers, that take care of all tasks: JSONL file download, Parquet export,...

The following exports are yet to migrate:

- CSV (en, fr)
- RDF (en, fr)
- daily diffs

We're also considering to move JSONL and MongoDB exports from Product Opener to this service, so that Product Opener does not handle exports anymore.

## Find the data downloads

Each data page lists the available download formats and reuse terms.

- [Open Food Facts](https://world.openfoodfacts.org/data) for food products
- [Open Beauty Facts](https://world.openbeautyfacts.org/data) for cosmetics
- [Open Products Facts](https://world.openproductsfacts.org/data) for other products
- [Open Pet Food Facts](https://world.openpetfoodfacts.org/data) for pet food

Start with a full database export if you need a slice such as products with
untaxonomized brands. JSONL has one product per line and can be streamed;
for column-oriented analysis of the food database, use the Parquet dataset
linked above.
