Downlinks
=========

It is possible for an agent to open downlinks to lanes on other agents (which are possibly on other hosts). There are
currently three supported kinds of downlink.

1. Event Downlinks: These allow you to observe changes to a single value on a remote lane.
2. Value Downlinks: These allow you to observe changes to a single value on a remote lane and keep a copy of the latest
   value. It is also possible to update the remote value from the downlink.
3. Map Downlinks: These allow you to track the state of a remote lane that consists of a map from keys to values. It is
   also possible to insert and remove items from the remote map using the downlink.

Downlink lifecycles
------------------

Downlinks have lifecycles in the same way that lanes or agents do. The lifecycle events that are supported by downlinks
are similar to hose supported by lanes of the similar type. Additionally, downlinks also have events that correspond
with the link being established our closed.

The common events shared by all downlinks are:

1. `on_linked`: This is triggered when the link with the remote lane is successfully established. It takes no
   parameters.
2. `on_synced`: This is triggered when a consistent view of the state of the remote lane has been assembled in the
   downlink. It takes a reference to the state of the downlink as a parameter.
3. `on_unlinked`: This is triggered when the link to the remote lane is closed. It takes no parameters.
4. `on_failed`: This is triggered when the link the remote lane fails with an error. It takes no parameters.

As downlinks have a single type (whereas an agent can have multiple lanes with arbitrary types) it is not necessary to
use a macro to assemble a downlink lifecycle implementation. Instead, builder types are provided to specify the event
handlers.

Building a stateless downlink
-----------------------------
In this section, we will construct a lifecycle for a value downlink that does not share any state between its handlers.
We will assume an agent with the following definitions:

```rust
#[projections]
#[derive(AgentLaneModel)]
struct ExampleAgent {
    received: ValueLane<String>,
}

struct ExampleLifecycle;

#[lifecycle(ExampleAgent)]
impl ExampleLifecycle {

    // Agent event handlers.

}
```

### Basic downlink

The simplest possible downlink lifecycle has no handlers bound to any of its events and can be built, using
the `HandlerContext` as below.

```rust
pub fn make_downlink(
    context: HandlerContext<ExampleAgent>,
) -> impl OpenValueDownlink<ExampleAgent, String> {
    let builder = context.value_downlink_builder::<String>(
        Some("swimos://example.remote:8080"),
        "/node",
        "lane",
        ValueDownlinkConfig::default(),
    );

    builder.done()
}
```

This function will create a `HandlerAction` that will attempt to open a downlink of type `String` to lane `"lane"` or
the agent at `"/node"` on the host `"swim://example.remote:8080"`. Here `OpenValueDownlink<..>` is a shorthand for
a `HandlerAction` that results in a value of type:

```rust
swimos::agent::agent_model::downlink::ValueDownlinkHandle
```

This handle can be used to set the value of the remote lane, through the downlink.

Opening the connection to the remote downlink is a blocking operation so it cannot run "inline" within an event handler.
Instead it is suspended as a future (as with the `suspend_future` combinator) that will attempt to make the connection
and then start running the lifecycle if it succeeds. The event handler will result in the handle immediately but it will
not have an effect until the downlink starts running.

### Adding event handlers

We can now start to add custom behaviour to the downlink. First, we will add a simply handler for the `on_linked` event:

```rust
pub fn make_downlink(
    context: HandlerContext<ExampleAgent>,
) -> impl OpenValueDownlink<ExampleAgent, String> {
    let builder = context.value_downlink_builder::<String>(
        Some("swimos://example.remote:8080"),
        "/node",
        "lane",
        ValueDownlinkConfig::default(),
    );

    builder
        .on_linked(my_downlink_linked)
        .done()
}

fn my_downlink_linked(context: HandlerContext<ExampleAgent>) -> impl EventHandler<ExampleAgent> {
    context.effect(|| println!("Downlink linked."))
}
```

Note that the downlink will run entirely within the context of the owning agent. Particularly, event handlers for the
downlink will be run in exactly the same way as event handlers belonging to the agent's own lifecycle. Particularly, the
downlink event handlers have full access to the agent. This allows use to attach a handler to the downlink that can
write into the lane of the agent:

```rust
pub fn make_downlink(
    context: HandlerContext<ExampleAgent>,
) -> impl OpenValueDownlink<ExampleAgent, String> {
    let builder = context.value_downlink_builder::<String>(
        Some("swimos://example.remote:8080"),
        "/node",
        "lane",
        ValueDownlinkConfig::default(),
    );

    builder
        .on_linked(my_downlink_linked)
        .on_event(my_downlink_event)
        .done()
}

//...

fn my_downlink_event(
    context: HandlerContext<ExampleAgent>,
    value: &str,
) -> impl EventHandler<ExampleAgent> {
    context.set_value(ExampleAgent::RECEIVED, value.to_string())
}
```

Each time a value is received on the downlink, it will be set as the value of the `received` lane.

Building a stateful downlink
----------------------------

As with agent lifecycles, it is possible to add state to a downlink lifecycle. As an example, we can extend the
downlink lifecycle from the previous section to set the value of the `received` lane to be the _previous_ value that was
received by the downlink.

As with agent lifecycles, the downlink lifecycle can only access its state through a shared reference. Therefore, we
must make use of interior mutability.

```rust
pub fn make_downlink(
    context: HandlerContext<ExampleAgent>,
) -> impl OpenValueDownlink<ExampleAgent, String> {
    let builder = context.value_downlink_builder::<String>(
        Some("swimos://example.remote:8080"),
        "/node",
        "lane",
        ValueDownlinkConfig::default(),
    );

    let state: RefCell<Option<String>> = Default::default();

    builder
        .on_linked(my_downlink_linked)
        .with_state(state)
        .on_event(my_downlink_event)
        .done()
}

//...

fn my_downlink_event(
    state: &RefCell<Option<String>>,
    context: HandlerContext<ExampleAgent>,
    value: &str,
) -> impl EventHandler<ExampleAgent> {
    state.replace(Some(value.to_string())).map(|previous| {
        context.set_value(ExampleAgent::RECEIVED, previous)
    }).discard()
}
```

Note: The `discard` combinator throws away the result of the action (here of type `Option<()>`) giving
an `EventHandler`, as required.

Opening a downlink
------------------

Downlinks are opened by executing the event handler produced by using the lifecycle builder. Referring to the example
given in the previous section, the downlink could be opened from the `on_start` event of the agent by:

```rust
#[on_start]
fn open_downlink_on_start(
    &self,
    context: HandlerContext<ExampleAgent>) -> impl EventHandler<ExampleAgent> {
    make_downlink(context).discard()
}
```

How downlinks run
-----------------

Downlinks are run in a similar way to suspended futures. An independent task runs concurrently in the background of the
agent. However, rather than resulting in a single `EventHandler` when the task completes, the task instead produces a
stream of `EventHandler`s for as long as the downlink continues to run.

As with suspended futures, these event handlers will necessarily be executed by dynamic dispatch.