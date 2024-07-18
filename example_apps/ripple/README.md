# [Ripple](ripple)

![Ripple](assets/ripple.png "Ripple")

Ripple is a real-time synchronous shared multiplayer experience built on the Swim platform.

See a hosted version of this app [here](https://ripple.swim.inc).

## Usage

Run the application using:

```shell
$ cargo run --bin ripple
```

Internally, this will spawn two servers: the Swim server which is running at localhost:9001 and a UI server running
at http://localhost:9002/index.html. Using your browser, navigate to http://localhost:9002/index.html and randomly click
on the screen to generate 'ripples'. These ripples are propagated to all users which are connected to the Swim server.
You may also hold your mouse's left button down to generate a 'charge' which is denoted by a larger circle displayed on
the screen.

This example application demonstrates a number of core Swim features:

- Value Lanes: To propagate the latest ripple.
- Map Lanes: To propagate the current charges that are happening.
- Command Lanes: For publishing new ripples and charges.
- Custom Event Types: Structures and Enumerations are used by Command Lanes for mutating the state of lanes.
- Event Handlers: Event Handlers are scheduled to run at random intervals to generate new ripples and prune any old
  charges that were not automatically removed if a user's browser suddenly closed.