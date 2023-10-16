Defining Agents in Rust
======================

This guide will tell you how to define your own Swim agents using the Rust API. It assumes that you are already familiar with the basic building blocks of a Swim application including:

* Agents
* Lanes
* Downlinks

If you don't know what any of these are, you should consult a more [general guide](https://www.swimos.org) first.

Contents
--------

1. [Defining Agents](define.md)
    * The `AgentLaneModel` trait.
    * Writing agent types.
    * The `Form` trait.
    * Persistence of lane state.
    * Private stores.
2. [Event Handlers](event_handler.md)
    * The `HandlerAction` and `EventHandler` traits.
    * How event handlers are executed.
    * The `HandlerContext`.
    * Handler combinators.
    * Suspending futures.
    * Unifying `EventHandler` types.
3. [Agent Lifecycles](lifecycle.md)
    * The `AgentLifecycle` trait.
    * Attribute macro to generate agent lifecycles.
    * Agent lifecycle events.
    * `CommandLane` events.
    * `DemandLane` events.
    * `DemandMapLane` events.
    * `ValueLane` events.
    * `MapLane` events.
    * `JoinValueLane` events.
    * `HttpLane` events.
    * Store events.
    * Borrowing from lifecycles.
    * Interior mutability in lifecycles.
4. [Downlinks](downlink.md)
    * Downlink lifecycles.
    * Building a stateless downlink.
    * Building a stateful downlink.
    * Opening a downlink.
    * How downlinks run.
5. [Agents from Scratch](advanced_agents.md)
    * TODO
6. [Advanced Use of Form Derivation](advanced_forms.md)
    * TODO