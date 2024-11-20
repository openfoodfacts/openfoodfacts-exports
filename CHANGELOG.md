# Changelog

## [0.4.0](https://github.com/openfoodfacts/openfoodfacts-exports/compare/v0.3.0...v0.4.0) (2024-11-20)


### Features

* add Open Prices export ([30c83a4](https://github.com/openfoodfacts/openfoodfacts-exports/commit/30c83a44b80b129d849da1231b90d5727587e720))
* export Open Beauty Facts data as parquet file ([#15](https://github.com/openfoodfacts/openfoodfacts-exports/issues/15)) ([095e0b1](https://github.com/openfoodfacts/openfoodfacts-exports/commit/095e0b1586a3ca6521db933d226ac816de8ac4aa))

## [0.3.0](https://github.com/openfoodfacts/openfoodfacts-exports/compare/v0.2.1...v0.3.0) (2024-11-18)


### Features

* add new fields to Parquet dump ([#12](https://github.com/openfoodfacts/openfoodfacts-exports/issues/12)) ([51ab153](https://github.com/openfoodfacts/openfoodfacts-exports/commit/51ab153fda91397b367cfabac29a5a20613067a8))


### Dependencies

* upgrade openfoodfacts package ([#13](https://github.com/openfoodfacts/openfoodfacts-exports/issues/13)) ([2e01519](https://github.com/openfoodfacts/openfoodfacts-exports/commit/2e015195ad9bbd98c7551e89865b8d1440bfebb9))

## [0.2.1](https://github.com/openfoodfacts/openfoodfacts-exports/compare/v0.2.0...v0.2.1) (2024-11-18)


### Bug Fixes

* add and update fields in parquet export ([#10](https://github.com/openfoodfacts/openfoodfacts-exports/issues/10)) ([5b99a3f](https://github.com/openfoodfacts/openfoodfacts-exports/commit/5b99a3fc2860cb55502cbc6992056850b42e5810))

## [0.2.0](https://github.com/openfoodfacts/openfoodfacts-exports/compare/v0.1.1...v0.2.0) (2024-11-15)


### Features

* :fire: Postprocess parquet: images field ([#6](https://github.com/openfoodfacts/openfoodfacts-exports/issues/6)) ([48e21b3](https://github.com/openfoodfacts/openfoodfacts-exports/commit/48e21b35312ef63f9cc3f913d1a8854ce5e00a51))
* declare the schema of the Parquet file ([#8](https://github.com/openfoodfacts/openfoodfacts-exports/issues/8)) ([309b4f7](https://github.com/openfoodfacts/openfoodfacts-exports/commit/309b4f73009424be65d712d2798f11bf7525d952))


### Bug Fixes

* add a validator to parse image sizes ([9dd2530](https://github.com/openfoodfacts/openfoodfacts-exports/commit/9dd25300b6d8c85004de39a1f1f36dab9f3e42fc))
* fix ignore_extra_sizes method ([bcc0e9b](https://github.com/openfoodfacts/openfoodfacts-exports/commit/bcc0e9be40ac8800136b063be24d943c33c7ac8e))
* increase job timeouts ([8b3b5cc](https://github.com/openfoodfacts/openfoodfacts-exports/commit/8b3b5cc420d404ae6effdc38892c330a0623e13b))
* increase row_group_size for Parquet generation ([5aaefa5](https://github.com/openfoodfacts/openfoodfacts-exports/commit/5aaefa5ee68083c6a7ffc8e4b0dbcfccd1fa1174))
* launch rq workers using CLI to init sentry ([2c7d0d7](https://github.com/openfoodfacts/openfoodfacts-exports/commit/2c7d0d76f16dd67cc2620f259c469a3a1afdf43e))
* use int64 for number_of_units ([824effa](https://github.com/openfoodfacts/openfoodfacts-exports/commit/824effa6565dede6faf68b60d999f6e12c5e6681))

## [0.1.1](https://github.com/openfoodfacts/openfoodfacts-exports/compare/v0.1.0...v0.1.1) (2024-11-12)


### Bug Fixes

* fix deployment & push dataset to Hugging Face ([#3](https://github.com/openfoodfacts/openfoodfacts-exports/issues/3)) ([b3c8f58](https://github.com/openfoodfacts/openfoodfacts-exports/commit/b3c8f58491007e3e8e6a4e266e1dacf90bcf02cf))

## 0.1.0 (2024-11-12)


### Features

* add first version of the codebase ([351c532](https://github.com/openfoodfacts/openfoodfacts-exports/commit/351c532e81f2f6f987fb4b0fa8e7b6b7d251baa7))
* add mobile app dump ([709e687](https://github.com/openfoodfacts/openfoodfacts-exports/commit/709e687184736c3a9938c1e91ca8105d4f64f4bc))
* add new command to launch export job ([7a1dcc7](https://github.com/openfoodfacts/openfoodfacts-exports/commit/7a1dcc769e11851749c9bff49ff58d7058999964))
* push mobile dataset to AWS S3 ([ebef351](https://github.com/openfoodfacts/openfoodfacts-exports/commit/ebef351881b39691398e44693d83211a1592fc51))
* try to export mobile dumps as gzipped TSV ([4ac0400](https://github.com/openfoodfacts/openfoodfacts-exports/commit/4ac04003dc350196a5f67a64174edd507fae497c))


### Bug Fixes

* add Sentry integration in launch-export CLI command ([666ec6c](https://github.com/openfoodfacts/openfoodfacts-exports/commit/666ec6c8f159848813a1cb111b9e759b691c68ad))
* don't download again the file if it didn't change ([70af15f](https://github.com/openfoodfacts/openfoodfacts-exports/commit/70af15fc3334812189169c7591d1fcc68e8f5dd8))
* don't use Path.rename if files are on different filesystems ([c11f827](https://github.com/openfoodfacts/openfoodfacts-exports/commit/c11f827d376a03ef96390f3848ec24b4b2d576aa))
* increase job timeout ([a3b6a1d](https://github.com/openfoodfacts/openfoodfacts-exports/commit/a3b6a1d63cce6e21d62721e1af872782ecea4a20))


### Dependencies

* add ruff as a dev dependency ([fa817c3](https://github.com/openfoodfacts/openfoodfacts-exports/commit/fa817c3192d4d49c498e7cc7611cb64d74756b80))


### Documentation

* add comment in container-build.yml ([27fd0a7](https://github.com/openfoodfacts/openfoodfacts-exports/commit/27fd0a7d2be48c4358db3dc4e7eea9e3fde2f825))
* add docstring ([c7f43d1](https://github.com/openfoodfacts/openfoodfacts-exports/commit/c7f43d1a7878b5ceaa2825795b450407cce2c191))
* update README.md ([e49daa1](https://github.com/openfoodfacts/openfoodfacts-exports/commit/e49daa17699956f50770df5da36defe7c7654935))
