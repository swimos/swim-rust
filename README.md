[![Build Status](https://travis-ci.com/swimos/swim-rust.svg?token=XRdC2qdFmdcvoFQjcbvN&branch=master)](https://travis-ci.com/swimos/swim-rust)
[![codecov](https://codecov.io/gh/swimos/swim-rust/branch/master/graph/badge.svg?token=IVWBLXCGW8)](https://codecov.io/gh/swimos/swim-rust)

## Development

### Unit tests

##### Basic: `cargo test`

##### With coverage:
1) `cargo install cargo-tarpaulin`
2) `cargo tarpaulin --ignore-tests -o Html -t 300`

### Lint

##### Manual
1) `cargo fmt -- --check`
2) `cargo clippy --all-features -- -D warnings`

##### Automatic (before commit): 
- Install hook: `sh ./install-commit-hook.sh`
- Remove hook: `sh ./remove-commit-hook.sh`