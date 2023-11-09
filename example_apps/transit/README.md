Transit Application
===================

This is a direct port of the Swim transit application that can be found [here](https://github.com/swimos/transit).

Running
-------

The application can be run (from the root directory of the transit project) with:

```
cargo run --bin transit -- --port 8001 --include-ui --ui-port 8002
```

The swim server will run on the port specified with `--port` and the web UI will be available at the port specified with `--ui-port`. Either of these can be omitted which will cause them to bind to any available port.

The web UI can then be found at:

```
http://127.0.0.1:8002/index.html
```

Logging
-------

The application has a default logging configuration which can be applied by passing `--enable-logging` on the command line. This will log directly to the console.

The default configuration may be altered using the standard `RUST_LOG` environment variable.