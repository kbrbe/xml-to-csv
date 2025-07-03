# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2025-07-03

### Fixed

- Parse wrongly encoded input if needed (added unit tests for it as well) ([#21](https://github.com/kbrbe/xml-to-csv/issues/21))

## [0.1.1] - 2025-04-01


### Changed

- Parse years between the year 0 and 1000

### Fixed

- Consistently use the specified identifierPrefix from the config in main CSV _and_ 1:n column CSV files ([#20](https://github.com/kbrbe/xml-to-csv/issues/20))

## [0.1.0] - 2025-02-07

### Added

- Initial version of the script which we use internally at KBR and at the MetaBelgica project already

[0.1.0]: https://github.com/kbrbe/xml-to-csv/releases/tag/v0.1.0
[0.1.1]: https://github.com/kbrbe/xml-to-csv/compare/v0.1.0...v0.1.1
[0.1.2]: https://github.com/kbrbe/xml-to-csv/compare/v0.1.1...v0.1.2
