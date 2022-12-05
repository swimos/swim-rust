Defining Agents in Rust
======================

This guide will tell you how to define your own Swim agents using the Rust API. It assumes that you are already familar with the basic building blocks of a Swim application including:

* Agents
* Lanes
* Downlinks

If you don't know what any of these are, you should consult a more general guide first.

Contents
--------

1. [Defining Agents](define.md)
    * The `AgentLaneModel` trait.
    * Writing agent types.
    * The `Form` trait.
    * Peristence of lane state.
    * Private stores.
2. [Event Handlers](event_handler.md)
    * The `HandlerAction` and `EventHandler` traits.
    * How event handlers are executed.
    * Handler combinators.
    * Suspending futures.
    * Interacting with lane state.
    * The `HandlerContext`.
3. [Agent Lifecycles](lifecycle.md)
    * The `AgentLifecycle` trait.
    * Attribute macro to generate agent lifecycles.
    * Agent lifecycle events.
    * `CommandLane` events.
    * `ValueLane` events.
    * `MapLane` events.
    * Borrowing state from lifecycles.
    * Interior mutability in lifecycles.
    * Sharing handler functions.
4. [Downlinks](downlink.md)
    * Opening a downlink.
    * Downlink lifecycles.
    * How downlinks run.
5. [Agents from Scratch](advanced_agents.md)
    * TODO
6. [Advanced Use of Form Derivation](advanced_forms.md)
    * TODO