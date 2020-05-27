use std::sync::Once;
use tracing::Level;
use tracing_subscriber::EnvFilter;

static INIT: Once = Once::new();

/// Add a tracing subscriber for the given directives to see their trace messages.
///
/// # Arguments
///
/// * `directives`             - The trace spans that you want to see messages from.
pub fn init_trace(directives: Vec<&str>) {
    INIT.call_once(|| {
        let mut filter = EnvFilter::from_default_env();

        for directive in directives {
            filter = filter.add_directive(directive.parse().unwrap());
        }

        tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .with_env_filter(filter)
            .init();
    });
}
