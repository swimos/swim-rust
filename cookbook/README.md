# Cookbook

[Cookbook](https://swimos.org/tutorials/) examples, implemented using the rust server and client.

## Running a Rust example

Running any cookbook example requires running at least one Rust file.

### Option 1: Build and Run With IDE

Simply navigate to your desired file(es), and run them through your IDE.

### Option 2: Build and Run from a terminal

Every cookbook has a Swim client. To run the client for a given cookbook, simply issue `cargo run -p $COOKBOOK_NAME --bin client` from your command line (make sure that you are in the `/cookbook` directory). For example, `cargo run -p downlinks --bin client` runs the Downlinks cookbook client.

For a cookbook demonstration that requires running multiple files, you will find a separate files for the server and the client. All cookbook-specific READMEs, found in their appropriate child directories, outline how to run these tasks. For example, to fully run the Command Lanes cookbook, you must issue both a `cargo run -p command_lanes --bin server` and a `cargo run -p command_lanes --bin client` from this directory.

