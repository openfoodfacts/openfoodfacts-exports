---
language:
- en
- fr
- de
- es
- it
- nl
- pl
- pt
- sv
- bg
- ro
- fi
- ru
- nb
- cs
- th
- da
- hr
- hu
- ar
- el
- ja
- ca
- sr
- sl
- sk
- tr
- lt
- zh
- et
- lv
- xx
- uk
- id
- he
- vi
- is
- la
- in
- ko
- sq
- iw
- ka
- ms
- bs
- fa
- bn
- gl
- kk
- mk
- nn
- hi
- aa
- uz
- so
- af
- eu
license:
- agpl-3.0
- odbl
size_categories:
- 1M<n<10M
pretty_name: Open Food Facts Product Database
dataset_info:
  config_name: default
configs:
- config_name: default
  data_files:
  - split: food
    path: food.parquet
  - split: beauty
    path: beauty.parquet
---

# Open Food Facts Database

## What is 🍊 Open Food Facts?

### A food products database

Open Food Facts is a database of food products with ingredients, allergens, nutrition facts and all the tidbits of information we can find on product labels.

### Made by everyone
Open Food Facts is a non-profit association of volunteers. 25.000+ contributors like you have added 1.7 million + products from 150 countries using our Android or iPhone app or their camera to scan barcodes and upload pictures of products and their labels.

### For everyone
Data about food is of public interest and has to be open. The complete database is published as open data and can be reused by anyone and for any use. Check-out the cool reuses or make your own!

## The Parquet Dataset

This dataset is a simpler version of the [JSONL dump](https://world.openfoodfacts.org/data) provided by the Open Food Facts organization on a daily basis. It was converted into the Parquet format for easy of use.

### Data processing

* `Debug` tags were removed.
* `Tags`tags are conserved since they contain most information,
* `Hierarchy` tags were removed
* `lc` tags were removed. It corresponds to the ["language of the interface"](https://openfoodfacts.github.io/openfoodfacts-server/reference/api-tutorials/adding-missing-products/#sending-the-right-country-and-language-parameters-based-on-the-country-your-user-is-located-in-and-the-language-the-product-is-in),
* `langs` tags are kept for each `ingredients_text` and conserved as individual columns (*for now*).

The original JSONL dump was processed using [Pyarrow](https://arrow.apache.org/docs/python/).


## Conditions for reuse

The Open Food Facts database is available under the Open Database License.
The individual contents of the database are available under the Database Contents License.
Products images are available under the Creative Commons Attribution ShareAlike licence. They may contain graphical elements subject to copyright or other rights, that may in some cases be reproduced (quotation rights or fair use).

Please read Terms and conditions of use and re-use before re-using the data.

## Tell us about your reuse
We are very interested in learning what the Open Food Facts data is used for. It is not mandatory, but we would very much appreciate it if you tell us about your re-uses so that we can share them with the Open Food Facts community. You can also fill this form to get a chance to get your app featured.

- **Homepage:** https://world.openfoodfacts.org/
- **Repository:** https://github.com/openfoodfacts
- **Point of Contact:** contact@openfoodfacts.org
