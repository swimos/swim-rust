Defining Agents
==============

The `AgentLaneModel` trait
--------------------------

To create an agent it is necessary to produce a type that implements the `swimos::agent::AgentLaneModel` trait. This is
defined as:

```rust
pub trait AgentLaneModel: Sized + Send {

    //... methods
}
```

The details of the methods of the trait are unimportant, for now, as implementations will typically be produced using a
derive macro (which will be covered later). Note, however, that any implementation must be `Send`. This restriction is
as the agent will be owned by an async task, within the runtime, which could potentially be moved between threads. This
implies that the types of data held by all lanes of the agent must also be `Send`.

Writing agent types
-------------------

The derive macro for `AgentLaneModel` can be applied to any struct type where all of the fields are lane types or store
types. So far, the supported lane types are:

* Value lanes: `swimos::agent::lanes::ValueLane`.
* Command lanes: `swimos::agent::lanes::CommandLane`.
* Demand lanes: `swimos::agent::lanes::DemandLane`.
* Demand Map lanes: `swimos::agent::lanes::DemandMapLane`.
* Map Lanes: `swimos::agent::lanes::MapLane`.
* Join Value Lanes: `swimos::agent::lanes::JoinValueLane`.
* Join Map Lanes: `swimos::agent::lanes::JoinMapLane`.
* HTTP Lanes: `swimos::agent::lanes::HttpLane` (or the shorthand `swimos::agent::lanes::SimpleHttpLane`).
* Supply Lanes: `swimos::agent::lanes::SupplyLane`.

The supported store types are:

* Value stores: `swimos::agent::stores::ValueStore`.
* Map stores: `swimos::agent::stores::MapStores`.

An example of a valid agent struct is:

```rust
use swimos::agent::AgentLaneModel;
use swimos::agent::lanes::*;

#[derive(AgentLaneModel)]
struct ExampleAgent {
    value_lane: ValueLane<i32>,
    command_lane: CommandLane<String>,
    map_lane: MapLane<String, u64>,
}
```

As mentioned above, all of the type parameters used in the lane types must be `Send` (which is clearly true
for `i32`, `String`, and `u64`). However, to use the derive macro there is a further restriction.

For the macro to be able to generate the implementation, it needs to know how to serialize and deserialize the types use
in the lane. This is encoded by the `swimos::Form` trait which is covered in the following section. Additionally,
for a map-like item (`MapLane<K, V>`, `MapStore<K, V>`, `JoinValueLane<K, V>`, `JoinMapLane<L, K, V>`) the key type `K`
must additionally satisfy:

```rust
K: Eq + Hash + Ord + Clone
```

For `JoinMapLane`s, the link key type `L` must satisfy:

```rust
L: Eq + Hash + Clone
```

Stores are effectively private alternatives to lanes. The maintain state in exactly the same was as the corresponding
lane types but are not exposed externally.

The `Form` trait
----------------

The `swimos::Form` trait tells the Swim framework how to serialize and deserialize a type. It is defined for a
wide
range of standard types including.

* Integers: `i32, i64, u32, u64, usize`.
* The unit type.
* Booleans.
* Strings (`String` and `Box<str>`).
* `f64`.
* Byte arrays (`Vec<u8>` and `Box<[u8]>`).
* `Vec<T>` where `T: Form`.
* `HashMap<K, V>` where `K: Form: Form + Hash + Eq` and `V: Form`.
* `BTreeMap<K, V>` where `K: Form + Ord + Eq` and `V: Form`.

There is also a derive macro that can be used to implement `Form` for your own struct and enum types (where all of the
types of the fields implement `Form`). For example:

```rust
use swimos_form::Form;

#[derive(Form)]
struct MyType {
    name: String,
    offset: i64,
}
```

There are a number of attributes that can be used with the `Form` derive macro to control the format of the
serialization. These attributes are covered in more detail in the [advanced forms](advanced_forms.md) chapter.

Persistence of lane state
-------------------------
If the server defines a persistent store, by default the state of every lane in an agent will be saved. This means that
if the agent is stopped and then later restarted, the state of the lanes will be restored.

However, in some cases it maybe be desirable to disable this. Flushing the state of the lane to disk has adds a
significant overhead. Additionally, some data maybe become stale and irrelevant if the agent is stopped for any length
of time. To achieve this using the derive macro, a tag can be added to the lane that should be be persisted.

```rust
#[derive(AgentLaneModel)]
struct ExampleAgent {
    #[item(transient)]
    value_lane: ValueLane<i32>,
    map_lane: MapLane<String, u64>,
}
```

In the above example, `value_lane` will not be persisted whereas `map_lane` will. For lanes that have no state (such
as `CommandLane`s, this annotations will have no effect).

Private stores
--------------

TODO: Agent private stores are not yet implemented.
