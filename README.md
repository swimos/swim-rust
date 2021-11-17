[![Build Status](https://travis-ci.com/swimos/swim-rust.svg?token=XRdC2qdFmdcvoFQjcbvN&branch=main)](https://travis-ci.com/swimos/swim-rust)
[![codecov](https://codecov.io/gh/swimos/swim-rust/branch/main/graph/badge.svg?token=IVWBLXCGW8)](https://codecov.io/gh/swimos/swim-rust)
<a href="https://www.swimos.org"><img src="https://docs.swimos.org/readme/marlin-blue.svg" align="left"></a>
<br><br><br>

## Examples

The [cookbook](/cookbook) can serve as a starting point for getting familiar with core Swim concepts that form the basis of all Swim apps.

## Demo applications
A number of demo applications are available in the [swim-rust-demos](https://github.com/swimos/swim-rust-demos) repository.

## Development

### Dependencies
[Formatting](https://github.com/rust-lang/rustfmt): `rustup component add rustfmt`<br>
[Clippy](https://github.com/rust-lang/rust-clippy): `rustup component add clippy`<br>
[Tarpaulin](https://github.com/xd009642/tarpaulin) `cargo install cargo-tarpaulin`<br>

### Unit tests
##### Basic: `cargo test`
##### With coverage: `cargo tarpaulin --ignore-tests -o Html -t 300`

### Lint
##### Manual
1) `cargo fmt --all -- --check`
2) `cargo clippy --all-features --workspace -- -D warnings`

##### Automatic (before commit): 
- Install hook: `sh ./install-commit-hook.sh`
- Remove hook: `sh ./remove-commit-hook.sh`