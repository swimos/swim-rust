Agent Lifecycles
================

For any given agent definition there are a number of different events to which event handlers can be attached. Some of these will receive one or more arguments and some receive none.

The `AgentLifecycle` trait
-------------------------

To attach event handlers to an agent implementation, it is necessary to provide an implementation of the `swim::agent::AgentLifecycle` trait.

The details of implementing this trait are somewhat complex and will be covered in a later chapter. In general, lifecycles will be created using the `swim::agent::lifecycle` attribute macro.

All further examples in this chapter will use the following agent definition:

```rust
use swim::agent::{AgentLaneModel, projections};

#[derive(AgentLaneModel)]
#[projections]
struct ExampleAgent {
    example_command: CommandLane<i32>,
    example_demand: DemandLane<i32>,
    example_demand_map: DemandMapLane<String, i32>,
    example_value: ValueLane<i32>,
    example_map: MapLane<String, i32>,
    example_join_value: JoinValueLane<String, i32>,
    example_join_map: JoinMapLane<String, String, i32>,
    example_http: SimpleHttpLane<String>,
}
```

The derivation of an agent lifecycle for this agent will follow this template:

```rust
use swim::agent::lifecycle;

#[derive(Clone, Copy)]
struct ExampleLifecycle;

#[lifecycle(ExampleAgent)]
impl ExampleLifecycle {

    //Definitions of event handlers.

}
```

With this annotation, the `ExampleLifecycle` type will now have a function with the signature:

```rust
pub fn into_lifecycle(self) -> impl AgentLifecycle + Clone + Send + 'static;
```

Agent lifecycle events
----------------------

Regardless of what lanes the agent has, it will have the following events.

1. The `on_start` event. This is triggered exactly once when the agent starts and receives no arguments.
2. The `on_stop` event. This is triggered just before the agent stops and receives no arguments. It is the last event handler be run by an agent (aside from any that are triggered by the event handler attached to the `on_stop` event.)

To attach a handler to the `on_start` method, add a function annotated with `#[on_start]`, with the following signature.

```rust
#[on_start]
fn my_start_handler(&self, context: HandlerContext<ExampleAgent>) -> impl EventHandler<ExampleAgent> {
    context.effect(|| println!("Starting agent."))
}
```

The `on_stop` event is identical but uses the `#[on_stop]` annotation.

Note that a function (with an appropriate signature) may have any number of lifecycle annotations. For example, the following is entirely acceptable:

```rust
#[on_start]
#[on_stop]
fn my_start_or_stop_handler(&self, context: HandlerContext<ExampleAgent>) -> impl EventHandler<ExampleAgent> {
    context.effect(|| println!("Starting or stopping agent."))
}
```

Additionally, for each lane of the agent, there are events that can be attached to that lane, using its name. For example, to annotate a function to be used for the `on_command` event of the command lane `example_command`, the annotation `#[on_command(example_command)]` would be used.

Command lane events
-------------------

The command lane has only one type of event that triggers any time it receives a command. The `on_command` event has the following signature:

```rust
#[on_command(example_command)]
fn my_command_handler(&self, context: HandlerContext<ExampleAgent>, value: &i32) -> impl EventHandler<ExampleAgent> {
    //...
}
```

Demand lane events
-------------------

The demand lane has only one type of event that triggers when it is explicitly cued or
an external sync request is received. In contrast to the other types of lane, this
handler produces a value rather than consumes one. This value will then be sent on
all connected uplinks (or just to the requester for a sync operation).

The default `on_cue` handler will produce an error and cause the agent to fail.

```rust
#[on_cue(example_demand)]
fn my_demand_handler(&self, context: HandlerContext<ExampleAgent>) -> impl HandlerAction<ExampleAgent, Completion = i32> {
    //...
}
```

Demand Map lane events
----------------------

A demand map lane has two types of event that generate its contents, on demand.

1. `on_cue_key`: This triggers each time a key of the map is cued to be generated. This occurs upon a manual cue operation and once for each defined key when a downlink attempts to sync with the lane.
2. `keys`: This triggers each time a downlink attempts to sync with the lane. It generates the set of keys that are defined.

The `on_cue_key` event has the following signature:

```rust
#[on_cue_key(example_demand_map)]
fn my_demand_handler(&self, context: HandlerContext<ExampleAgent>, key: String) -> impl HandlerAction<ExampleAgent, Completion = Option<i32>> {
    //...
}
```

The `keys` event has the following signature:

```rust
#[keys(example_demand_map)]
fn my_demand_keys_handler(&self, context: HandlerContext<ExampleAgent>) -> impl HandlerAction<ExampleAgent, Completion = HashSet<i32>> {
    //...
}
```

Supply lane events
-------------------

Supply lanes generate no events on a lifecycle.

Value lane events
-----------------

A value lane generates events each time a new value is set. The two events that are generated are.

1. `on_event`: This triggers first and only receives the new value that was set.
2. `on_set`: This triggers second and also receives the old value that was replaced.

This signatures of these two events are as follows:

```rust
#[on_event(example_value)]
fn my_event_handler(&self, context: HandlerContext<ExampleAgent>, new_value: &i32) -> impl EventHandler<ExampleAgent> {
    //...
}

#[on_set(example_value)]
fn my_set_handler(&self, context: HandlerContext<ExampleAgent>, new_value: &i32, previous_value: Option<i32>) -> impl EventHandler<ExampleAgent> {
    //...
}
```

Map lane events
---------------

A map lanes generates events in the following three circumstances:

1. `on_update`: Triggered when an entry in the map is updated. The handler will receive a reference to the contents of the map (after the update), the key that was changed, the previous value (if any) and a reference to the new value.
2. `on_remove`: Triggered when an entry is removed. The handler will receive a reference to the contents of the map (after the removal), the key that was removed and the previous value.
3. `on_clear`: Triggered when the contents of the map are cleared. The handler will receive the previous contents of the map.

The signatures of these events are as follows:

```rust
#[on_update(example_map)]
fn my_update_handler(
    &self, 
    context: HandlerContext<ExampleAgent>,
    map: &HashMap<String, i32>,
    key: String,
    previous_value: Option<i32>,
    new_value: &i32) -> impl EventHandler<ExampleAgent> {
    //...
}

#[on_remove(example_map)]
fn my_remove_handler(
    &self, 
    context: HandlerContext<ExampleAgent>,
    map: &HashMap<String, i32>,
    key: String,
    previous_value: i32) -> impl EventHandler<ExampleAgent> {
    //...
}

#[on_clear(example_map)]
fn my_clear_handler(
    &self, 
    context: HandlerContext<ExampleAgent>,
    previous_map: HashMap<String, i32>) -> impl EventHandler<ExampleAgent> {
    //...
}
```

Join value lane events
----------------------

A join value lane supports all of the events supported by a map lane (`on_update`, `on_remove` and `on_clear`).

Additionally, it is possible to attach events to the downlinks used for each key of the join value lane. To do this, add the following to the agent lifecycle implementation:

```rust
#[join_value_lifecycle(example_join_value)]
fn register_lifecycle(
    &self,
    context: JoinValueContext<ExampleAgent, String, i32>,
) -> impl JoinValueLaneLifecycle<String, i32, ExampleAgent> + 'static {
    context.builder()
        .on_linked(|context, key, address| context.effect(move || {
            println!("Linked downlink for key: {} from lane at: {}", key, address);
        }))
        .done()
}
```

An instance of the lifecycle will be created for each link that is attached to the lane so it is necessary that the lifecycle is `Clone` and has a `static` lifetime.

Supported handlers are `on_linked`, `on_synced`, `on_unlinked` and `on_failed`. These take closures with the following signatures:

1. `on_linked`: 
```rust
(context: HandlerContext<ExampleAgent>, key: K, address: Address<&str>) -> impl EventHandler<ExampleAgent>
```
2. `on_synced`:
```rust
(context: HandlerContext<ExampleAgent>, key: K, address: Address<&str>, value: Option<&V>) -> impl EventHandler<ExampleAgent>
```
3. `on_unlinked` and `on_failed`:
```rust
(context: HandlerContext<ExampleAgent>, key: K, address: Address<&str>) -> impl HandlerAction<ExampleAgent, Completion = LinkClosedResponse>
```

The `on_unlinked` and `on_failed` handlers return a `LinkClosedResponse`. This is an enumeration with the values `Retry`, `Abandon` and `Delete`. These have the following effects:

1. `Retry`: An attempt will be made to reconnect the link (TODO: this is not implemented yet.)
2. `Abandon`: The link will be abandoned by the entry will still remain in the map. (This is the default.)
3. `Delete`: The link will be abandoned and the entry deleted from the map.

It is possible to add a shared state to a join value lifecycle in a similar way to other downlinks types (see [Downlinks](downlink.md)). Note that this state is shared between the event handlers of a single instance of the lifecycle and not between all instances. If you have state that needs to be shared between all instances it must be stored inside an `Arc`.

Join map lane events
--------------------

A join map lane supports all of the events supported by a map lane (`on_update`, `on_remove` and `on_clear`).

Additionally, it is possible to attach events to the downlinks used for each key of the join value lane. To do this, add the following to the agent lifecycle implementation:

```rust
#[join_map_lifecycle(example_join_map)]
fn register_lifecycle(
    &self,
    context: JoinMapContext<ExampleAgent, String, String, i32>,
) -> impl JoinMapLaneLifecycle<String, String, ExampleAgent> + 'static {
    context.builder()
        .on_linked(|context, link_key, address| context.effect(move || {
            println!("Linked downlink for link: {} from lane at: {}", key, address);
        }))
        .done()
}
```

An instance of the lifecycle will be created for each link that is attached to the lane so it is necessary that the lifecycle is `Clone` and has a `static` lifetime.

Supported handlers are `on_linked`, `on_synced`, `on_unlinked` and `on_failed`. These take closures with the following signatures:

1. `on_linked`: 
```rust
(context: HandlerContext<ExampleAgent>, link_key: L, address: Address<&str>) -> impl EventHandler<ExampleAgent>
```
2. `on_synced`:
```rust
(context: HandlerContext<ExampleAgent>, link_key: L, address: Address<&str>, keys: &HashSet<K>) -> impl EventHandler<ExampleAgent>
```
3. `on_unlinked` and `on_failed`:
```rust
(context: HandlerContext<ExampleAgent>, link_key: L, address: Address<&str>, keys: HashSet<K>) -> impl HandlerAction<ExampleAgent, Completion = LinkClosedResponse>
```

The `on_unlinked` and `on_failed` handlers return a `LinkClosedResponse`. This is an enumeration with the values `Retry`, `Abandon` and `Delete`. These have the following effects:

1. `Retry`: An attempt will be made to reconnect the link (TODO: this is not implemented yet.)
2. `Abandon`: The link will be abandoned by the entry will still remain in the map. (This is the default.)
3. `Delete`: The link will be abandoned and the entry deleted from the map.

It is possible to add a shared state to a join value lifecycle in a similar way to other downlinks types (see [Downlinks](downlink.md)). Note that this state is shared between the event handlers of a single instance of the lifecycle and not between all instances. If you have state that needs to be shared between all instances it must be stored inside an `Arc`.

HTTP lane events
----------------

An HTTP lane generates events when HTTP requests are received for that lane. The supported methods are GET, PUT, POST, DELETE and HEAD. The event handlers used to serve these methods are named.

1. `on_get` for GET.
2. `on_put` for PUT.
3. `on_post` for POST.
4. `on_delete` for DELETE.

When a HEAD request is received, the `on_get` handler will be called and then the payload of the response will be discarded.
The signatures of these events are as follows:

```rust
#[on_get(example_http)]
fn my_get_handler(
    &self, 
    context: HandlerContext<ExampleAgent>,
    http_context: HttpRequestContext) -> impl HandlerAction<ExampleAgent, Completion = Response<String>> {
    //...
}

#[on_put(example_http)]
fn my_put_handler(
    &self, 
    context: HandlerContext<ExampleAgent>,
    http_context: HttpRequestContext,
    value: String) -> impl HandlerAction<ExampleAgent, Completion = UnitResponse> {
    //...
}

#[on_post(example_http)]
fn my_post_handler(
    &self, 
    context: HandlerContext<ExampleAgent>,
    http_context: HttpRequestContext,
    value: String) -> impl HandlerAction<ExampleAgent, Completion = UnitResponse> {
    //...
}

#[on_delete(example_http)]
fn my_delete_handler(
    &self, 
    context: HandlerContext<ExampleAgent>,
    http_context: HttpRequestContext) -> impl HandlerAction<ExampleAgent, Completion = UnitResponse> {
    //...
}
```

The `HttpRequestContext` passed to each of these handlers provides access to the request URI and the headers that were set in the request.

The `Response` type produced by the event handlers contains the payload, status code and any custom headers. It is the responsibility of the codec associated with the lane to interpret the content type and accept headers from the request and to append the correct content type header to the response. In most cases a response can be constructed as:

```rust
Response::from(value)
```

where `value` is the payload. The status code will be 200(OK) and no extra headers will be set. `UnitResponse` is equivalent to `Response<()>`.

Store events
------------

The store types support exactly the same event handlers as their lane equivalents (for example `ValueStore`s have the same events as `ValueLane`s).

Borrowing from lifecycles
-------------------------

Although none of the examples so far have used event handlers that borrow any data from the lifecycle, this is entirely possible.

Consider the following, alternative lifecycle type.

```rust
#[derive(Clone)]
struct NamedLifecycle {
    name: String
}

#[lifecycle(ExampleAgent)]
impl NamedLifecycle {

    // Definitions of event handlers.

}
```

We can attach an event to the value lane that uses the name as follows:

```rust
#[on_event(example_value)]
fn with_name_event<'a>(
    &'a self,
    context: HandlerContext<ExampleAgent>,
    value: &i32,
) -> impl EventHandler<ExampleAgent> + 'a {
    let n = *value;
    context.effect(move || {
        println!("name = {}, value = {}", self.name, n)
    })
}
```

Note that, whilst we can borrow `&self.name` safely, it is _not_ possible to move the borrow of `value` into the event handler. This is as the current value of the lane can change during the execution of the event handler so it is not possible to hold a borrow of it.

Interior mutability in lifecycles
---------------------------------

The reference to `self` for the agent lifecycle is always passed as `&self` and a function taking `&mut self` cannot be used for an event handler. This is as an event handler can trigger other handlers on the same lifecycle (even itself) which results in multiple borrows of the data in the lifecycle.

Often, it will not be possible for a stateful lifecycle to be `Clone` (as the state may not be cloneable). In such a case, the `no_clone` option to the lifecycle macro. When registering the agent in the server, it will be necessary to provide a factory rather than a template instance.

However, due to the way in which lifecycles are executed, only one event handler will be executing at any one time. This means that it is safe to use interior mutability in a lifecycle without requiring locking. Consider the following alternative lifecycle:

```rust
struct MutableLifecycle {
    list: RefCell<Vec<i32>>,
}

#[lifecycle(ExampleAgent, no_clone)]
impl MutableLifecycle {

    #[on_command(example_command)]
    fn add_to_list<'a>(
        &'a self,
        context: HandlerContext<ExampleAgent>,
        command: &i32) -> impl EventHandler<ExampleAgent> + 'a {
        let n = *command;
        context.effect(move || {
            self.list.borrow_mut().push(n);
        })
    }

}
```

This is guaranteed to never panic as only one mutable borrow of the list can exist at any time.