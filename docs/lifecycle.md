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
    example_value: ValueLane<i32>,
    example_map: MapLane<String, i32>,
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

Agent lifecycle events
----------------------

Regardless of what lanes the agent has, it will have the following events.

1. The `on_start` event. This is triggered exactly once when the agent starts and receives no arguments.
2. The `on_stop` event. This is triggered just before the agent stops and receives no arguments. It is the last event handler be run by an agent (aside from any that are triggered by the event handler attached to the `on_stop` event.)

To attach a handler to the `on_start` method, add a function annotated with `#[on_start]`, with the following signature.

```rust
#[on_start]
fn my_start_handler(&self, context: AgentContext<ExampleAgent>) -> impl EventHandler<ExampleAgent> {
    context.effect(|| println!("Starting agent."))
}
```

The `on_stop` event is identical but uses the `#[on_stop]` annotation.

Note that a function (with an appropriate signature) may have any number of lifecycle annotations. For example, the following is entirely acceptable:

```rust
#[on_start]
#[on_stop]
fn my_start_or_stop_handler(&self, context: AgentContext<ExampleAgent>) -> impl EventHandler<ExampleAgent> {
    context.effect(|| println!("Starting or stopping agent."))
}
```

Additionally, for each lane of the agent, there are events that can be attached to that lane, using its name. For example, to annotate a function to be used for the `on_command` event of the command lane `example_command`, the annotation `#[on_command(example_command)]` would be used.

Command lane events
-------------------

The command lane has only one type of event that triggers any time it receives a command. The `on_command` event has the following signature:

```rust
#[on_command(example_command)]
fn my_command_handler(&self, context: AgentContext<ExampleAgent>, value: &i32) -> impl EventHandler<ExampleAgent> {
    //...
}
```

Value lane events
-----------------

A value lane generates events each time a new value is set. The two events that are generated are.

1. `on_event`: This triggers first and only receives the new value that was set.
2. `on_set`: This triggers second and also receives the old value that was replaced.

This signatures of these two events are as follows:

```rust
#[on_event(example_value)]
fn my_event_handler(&self, context: AgentContext<ExampleAgent>, new_value: &i32) -> impl EventHandler<ExampleAgent> {
    //...
}

#[on_set(example_value)]
fn my_set_handler(&self, context: AgentContext<ExampleAgent>, new_value: &i32, previous_value: Option<i32>) -> impl EventHandler<ExampleAgent> {
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
    context: AgentContext<ExampleAgent>,
    map: &HashMap<String, i32>,
    key: String,
    previous_value: Option<i32>,
    new_value: &i32) -> impl EventHandler<ExampleAgent> {
    //...
}

#[on_remove(example_map)]
fn my_remove_handler(
    &self, 
    context: AgentContext<ExampleAgent>,
    map: &HashMap<String, i32>,
    key: String,
    previous_value: i32) -> impl EventHandler<ExampleAgent> {
    //...
}

#[on_clear(example_map)]
fn my_clear_handler(
    &self, 
    context: AgentContext<ExampleAgent>,
    previous_map: HashMap<String, i32>) -> impl EventHandler<ExampleAgent> {
    //...
}
```

Borrowing from lifecycles
-------------------------

Although none of the examples so far have used event handlers that borrow any data from the lifecycle, this is entirely possible.

Consider the following, alternative lifecycle type.

```rust
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

However, due to the way in which lifecycles are executed, only one event handler will be executing at any one time. This means that it is safe to use interior mutability in a lifecycle without requiring locking. Consider the following alternative lifecycle:

```rust
struct MutableLifecycle {
    list: RefCell<Vec<i32>>,
}

#[lifecycle(ExampleAgent)]
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