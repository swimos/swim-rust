<div style="text-align: center;">
    <img src="https://docs.swimos.org/readme/marlin-blue.svg" alt="swim_logo" width="200"/>
</div>

-----

# SwimOS - Rust: Form
 [![Crates.io][crates-badge]][crates-url] | [API Docs](https://docs.rs/swimos/latest/swim_form)

[crates-badge]: https://img.shields.io/crates/v/swim_form.svg
[crates-url]: https://crates.io/crates/swim_form

A `Form` transforms between a Rust object and a structurally typed `Value`, allowing for complex data types to be stored in Web Agents. Decorating a Rust object with `#[form(Value)]` derives a method to serialise the object to a `Value` and to deserialise it back into a Rust object. The argument to this macro is a name binding to the `Value` enumeration; this differs between the Swim Client and Swim Server crates and so must be specified. The form macro performs compile-time checking to ensure that all fields implement the `Form` trait. Forms are backed by Serde and so all of Serde's attributes work for serialisation and deserialisation. As a result, Serde must be included as a dependency with the `derive` feature enabled. 

The `Form` trait is implemented for: `f64`, `i32`, `i64`, `bool`, `String`, and `Option<V>` where `V` implements `Form`.

# Usage:
Add to your `Cargo.toml`:

```toml
[dependencies]
swim_form = "1.0"
serde = { version = "1.0", features = ["derive"] }
```

```rust
use swim_client::common::model::{Attr, Item, Value};
use swim_form::*;

fn main() {
    #[form(Value)]
    #[derive(PartialEq, Debug)]
    struct Sample {
        a: i32,
    }

    let record = Value::Record(
        vec![Attr::from("Sample")], 
        vec![Item::from(("a", 1))]
    );

    let result = Sample::try_from_value(&record).unwrap();
    assert_eq!(result, Sample { a: 1 })
}

```