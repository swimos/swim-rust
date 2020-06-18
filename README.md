[![Build Status](https://travis-ci.com/swimos/swim-rust.svg?token=XRdC2qdFmdcvoFQjcbvN&branch=master)](https://travis-ci.com/swimos/swim-rust)
[![codecov](https://codecov.io/gh/swimos/swim-rust/branch/master/graph/badge.svg?token=IVWBLXCGW8)](https://codecov.io/gh/swimos/swim-rust)
<a href="https://www.swimos.org"><img src="https://docs.swimos.org/readme/marlin-blue.svg" align="left"></a>

## Development

### Dependencies
[Formatting](https://github.com/rust-lang/rustfmt): `rustup component add rustfmt`
[Clippy](https://github.com/rust-lang/rust-clippy): `rustup component add clippy`
[Tarpaulin](https://github.com/xd009642/tarpaulin) `cargo install cargo-tarpaulin`

### Unit tests
##### Basic: `cargo test`
##### With coverage: `cargo tarpaulin --ignore-tests -o Html -t 300`

### Lint
##### Manual
1) `cargo fmt -- --check`
2) `cargo clippy --all-features -- -D warnings`

##### Automatic (before commit): 
- Install hook: `sh ./install-commit-hook.sh`
- Remove hook: `sh ./remove-commit-hook.sh`