# Changelog

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
