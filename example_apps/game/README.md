Game Leaderboard Application
===================

This is a direct port of the Swim game application that can be found [here](https://github.com/swimos/demos).

Running
-------

The application can be run (from the root directory of the game project) with:

```
cargo run --bin game -- --port 9001 --include-ui --ui-port 9002
```

The swim server will run on the port specified with `--port` and the web UI will be available at the port specified with `--ui-port`. Either of these can be omitted which will cause them to bind to any available port.

The web UI can then be found at:

```
http://127.0.0.1:9002/index.html?host=ws://localhost:9001
```

Logging
-------

The application has a default logging configuration which can be applied by passing `--enable-logging` on the command line. This will log directly to the console.

The default configuration may be altered using the standard `RUST_LOG` environment variable.