Event Handlers
==============

After having defined the lanes and stores of the agent, we now want to add logic to be executed when particular events occur (for example, an event is fired each time an agent starts). Naively, we might expect that this could simply be a function that is called in response to the event. 

Consider the following agent definition:

```rust
#[derive(AgentLaneModel)]
struct MyAgent {
    add_name: CommandLane<String>,
    accumulate: ValueLane<Vec<String>>,
    length: ValueLane<u64>,
}
```

We want to attach the following behaviours:

1. When `add_name` receives a command, it appends that string to `accumulate`. It then reads the value of `length` and prints it.
2. When the value of `accumulate` is set, it writes its own length to the `length` lane.

These two event handlers are independent of each other. However, the behaviour we expect is that when the handler on `add_name` reads the value of `length`, it should see the total _after_ the event handler on `accumulate` has executed.

It is guaranteed that a single agent runs within one task in the asynchronous runtime and so we need not worry about the event handlers executing on separate threads. However, satisfying the above expectation implies that the execution of event handlers can be suspended to run other handlers.

To accomplish this, rather than being modelled as simple function, event handlers are modelled as state machines. In fact, the definition looks very similar to the `Future` trait in the standard library.

The `HandlerAction` and `EventHandler` traits
---------------------------------------------

Event handlers are modelled by the `HandlerAction` trait:

```rust
trait HandlerAction<AgentType> {

    type Completion;

    /// ... implementation details.  
}
```

The trait is parameterized on the type of the agent to allow handlers to interact with the lanes of the agent (by for example setting a new value). The `Completion` associated type is the type of the value that the action will produce when it completes. (It is also possible that an action could fail and instead produce an error).

An event handler is an action that does not result in any final value. Particularly:

```rust
trait EventHandler<Context>: HandlerAction<Context, Completion = ()> {}
```

Every type that implements `HandlerAction` with `Completion = ()` also implements `EventHandler` through a blanket implementation.

It is not necessary to consider the methods of the `HandlerAction` trait as it is not intended that you will need to implement it directly. Instead, event handlers can be built up by composing simple predefined actions, using a suite of handler combinators.

The canonical `EventHandler` that will do nothing is the `UnitHandler` which will immediately complete.

How event handlers are executed.
--------------------------------

The process of running an event handler is similar to polling a `Future`. The event handler is called, providing a context that allows it to access the lanes of the agent, and performs some work. It may then either:

1. Complete, ending the process.
2. Fail with an error. In this case all execution will stop and the agent will fail.
3. Yield back to the agent. In this case the handler will indicate if it has affected the state of any other lane in the agent.

In the third case, a check will be performed to determine if any event handlers are triggered on the lane that the handler has modified. If so, the process is then run recursively on _that_ handler until it completes or fails. Following that, execution of the original handler resumes.

Note that there is currently no logic to detect an  infinite chain of handler triggers. Therefore, any infinitely descending chain of events will exhaust the stack. It is intended to add heuristics to guard against this in future.

The `HandlerContext`.
-------------------------------------

The aid in the construction of event handlers, a factor type is provided in the form of:

```rust
swim::agent::agent_lifecycle::utility::HandlerContext<AgentType>
```

This allows for the easier construction of event handlers that will run in the context of the `AgentType`. Typically, in locations where an event handler is required, an instance of this type will be provided. (It is always possible to create event handlers directly, the utility simply aids with type inference and reduces the need for type ascriptions in the code).

The simplest possible event handler runs a side effect. For example:

```rust
fn make_handler(context: HandlerContext<AgentType>) -> impl EventHandler<AgentType> {
    context.effect(|| println!("Hello World!"))
}
```

An effect can be any function/closure satisfying `FnOnce()`.

Additionally, the context allows for the creation of event handlers that will interact with the lanes of the agent.

Consider the following simple agent definition:

```rust
#[derive(AgentLaneModel)]
struct Example {
    name: ValueLane<String>
}
```

To create an event handler that would set the value of the `name` lane, you would do the following:

```rust
fn set_name_handler(context: HandlerContext<Example>) -> impl EventHandler<Example> {
    context.set_value(|agent| &agent.name, "foo".to_string())
}
```

The first argument to the `set_value` method is a projection which indicates which lane that the handler should be applied to. For example, in this case it has the type:

```rust
fn(&Example) -> &ValueLane<String>
```

As projections of this kind will be required quite frequently, an attribute macro is provided to generate them for you. This could be applied to the previous example as follows:

```rust
use swim::agent::{AgentLaneModel, projections};

#[derive(AgentLaneModel)]
#[projections]
struct Example {
    name: ValueLane<String>
}
```

This is equivalent to adding an impl block like:

```rust
impl Example {

    const NAME: fn(&Example) -> &ValueLane<String> = |agent: &Example| &agent.name;

}
```

and allows the event handler to be written as:

```rust
fn set_name_handler(context: HandlerContext<Example>) -> impl EventHandler<Example> {
    context.set_value(Example::NAME, "foo".to_string())
}
```

Additional basic handlers are available to interact with other types of lane and to get the route URI of the running agent instance. See the documentation for `HandlerContext` for more details.

Handler combinators
-------------------

A number of combinators are provided to combine handler actions together. This are available as extensions methods using the `swim::agent::event_handler::HandlerActionExt` trait. As an example, consider the `and_then` combinator. When applied to a handler action this applies a closure to the result of that handler which, in turn produces another handler. When an `and_then` actions is executed, the first handler action runs to completion, the function is applied to the result, and execution continues with the resulting handler action. As an example:

```rust
fn print_uri_handler(context: HandlerContext<AgentType>) -> impl EventHandler<AgentType> {
    context.get_agent_uri().and_then(move |uri| context.effect(move || {
        println!("Agent is running at: {}", uri);
    }))
}
```

This compound handler will fetch the route URI at which the agent is running and then print it to the console.

For more details on the available combinators, look at the documentation for `HandlerActionExt`.

Suspending futures
------------------

Event handlers run synchronously and so should never block the thread that is running them. Blocking code in an event handler will cause the host agent to lock up and stop handling input.

This has the consequence that it is impossible to use an async function/closure as an event handler directly. However, it is possible for an event handler to suspend an asynchronous function that will execute in the background. When this future completes it will result in another event handler which will then be executed by the agent.

Futures can be suspended using the `HandlerContext`. Consider the following example:

```rust
async fn long_operation(context: HandlerContext<AgentType>) -> impl EventHandler<AgentType> {
    // ... Asynchronous operations.
    let result: i32 = ...;
    context.effect(move || {
        println!("Operation completed with: {}", result)
    })
}

fn make_handler()(context: HandlerContext<AgentType>) => impl EventHandler<AgentType> {
    context.suspend(long_operation(context))
}
```

This will suspend the function `long_operation` into the background where it will be executed concurrently with the rest of the agent. When it completes, it will create a header that prints the result of operation which will then be executed by the agent, as with any other event handler.

As arbitrary futures may be suspended in this way, the resulting event handlers are necessarily executed by dynamic dispatch.

Unifying `EventHandler` types
-----------------------------

All of the functions that associate an `EventHandler` with an event expect a return of:

```rust
impl EventHandler<AgentType>
```

This implies that the function is expected to return handlers of a single type only.

The `HandlerAction` trait is implemented for `Option<H>` where `H: HandlerAction<..>` (resulting in `Option<H::Completion>`).

For a fixed enumeration of `HandlerAction` types, it would be possible to manually implement the `HandlerAction` trait, however, in most cases this would be unnecessarily verbose.

The `EventHandler` trait is object safe so, in situations where a little overhead is acceptable, it is possible to return an event handler as:

```rust
pub type BoxEventHandler<'a, Context> = Box<dyn EventHandler<Context> + 'a>;
```

by simply boxing the various types of handler being returned.
