# SwimOS Development Guide

## Dependencies

[Formatting](https://github.com/rust-lang/rustfmt): `rustup component add rustfmt`<br>
[Clippy](https://github.com/rust-lang/rust-clippy): `rustup component add clippy`<br>
[Tarpaulin](https://github.com/xd009642/tarpaulin) `cargo install cargo-tarpaulin`<br>

## Unit tests

#### Basic: `cargo test`

#### With coverage: `cargo tarpaulin --ignore-tests -o Html -t 300`

## Lint

#### Manual

1) `cargo fmt --all -- --check`
2) `cargo clippy --all-features --workspace --all-targets -- -D warnings`

#### Automatic (before commit):

- Install hook: `sh ./install-commit-hook.sh`
- Remove hook: `sh ./remove-commit-hook.sh`

Note: The pre-commit hooks take a while to run all checks.