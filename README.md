[![Build Status](https://dev.azure.com/swimai-build/swim-rust/_apis/build/status/swimos.swim-rust?branchName=main)](https://dev.azure.com/swimai-build/swim-rust/_build/latest?definitionId=1&branchName=main)
[![codecov](https://codecov.io/gh/swimos/swim-rust/branch/main/graph/badge.svg?token=IVWBLXCGW8)](https://codecov.io/gh/swimos/swim-rust)
<a href="https://www.swimos.org"><img src="https://docs.swimos.org/readme/marlin-blue.svg" align="left"></a>
<br><br><br><br>

The Swim Rust SDK contains software framework for building stateful applications that can be interacted
with via multiplexed streaming APIs. It is built on top of the [Tokio asynchronous runtime](https://tokio.rs/)
and a Tokio runtime is required for any Swim application.

Each application consists of some number of stateful agents, each of which runs as a separate Tokio task
and can be individually addressed by a URI. An agent may have both public and private state which can either
be held solely in memory or, optionally, in persistent storage. The public state of the agent consists of a
number of lanes, analogous to a field in a record. There are multiple kinds of lanes that, for example, lanes
containing single values and those containing a map of key-value pairs.

The state of any lane can be observed by establishing a link to it (either from another agent instance or a
dedicated client). A established link will push all updates to the state of that lane to the subscriber and
will also allow the subscriber to request changes to the state (for lane kinds that support this). Links
operate over a web-socket connection and are multiplexed, meaning that links to multiple lanes on the same
host can share a single web-socket connection.

## Usage Guides

[Implementing Swim Agents in Rust](docs/agent.md)

[Building a Swim Server Application](docs/server.md)

## Examples

TODO
## Development

See the [development guide](DEVELOPMENT.md);