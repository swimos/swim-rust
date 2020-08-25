use crate::utils::{number_menu, ClientConfig, RetryStrategy};
use crate::{flush, get_input, print_title, show_help};

pub(crate) fn router_params_menu(client_config: &mut ClientConfig) {
    loop {
        print_title("Router parameters");
        println!("1. Retry strategy");
        println!("2. Idle timeout");
        println!("3. Connection reaper frequency");
        println!("4. Buffer size");
        print!("Select an option or `b` to go back: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "1" => router_retry_strategy(client_config),
            "2" => router_idle_timeout(client_config),
            "3" => router_conn_reaper_freq(client_config),
            "4" => router_buffer_size(client_config),
            "b" => break,
            _ => println!("Invalid selection \"{}\"!", input),
        }
    }
}

fn router_retry_strategy(client_config: &mut ClientConfig) {
    loop {
        print!("Enter a value for retry strategy, `h` for help or `b` to go back: ");
        flush();

        let input = get_input();

        let value = format!("{}\n{:<15} {}\n{:<15} {}\n{:<15} {}",
                            "* immediate - A retry strategy that does an immediate retry with no sleep time in between the requests.",
                            "",
                            "* interval - A retry strategy with a defined delay in between the requests.",
                            "",
                            "* exponential - A truncated exponential retry strategy.",
                            "",
                            "* none - A retry strategy that only attempts the request once.");

        match
        input.as_str() {
            "h" => show_help(
                "The retry strategy that will be used when attempting to make a request to a remote agent if a failure occurs.",
                &value,
                "exponential",
            ),
            "b" => break,
            "immediate" => {retry_immediate(client_config); break},
            "interval" => {retry_interval(client_config); break},
            "exponential" => {retry_exponential(client_config); break},
            "none" => {retry_none(client_config); break},
            _ => println!("Invalid value \"{}\" for retry strategy!", input),
        }
    }
}

fn retry_immediate(client_config: &mut ClientConfig) {
    let retries = number_menu(
        "number of retries",
        "The maximum number of retry attempts that will be made.",
        "5",
        true,
        true,
        false,
    );

    client_config.router_config.retry_strategy = Some(RetryStrategy::Immediate { retries });
}

fn retry_interval(client_config: &mut ClientConfig) {
    let delay = number_menu(
        "delay between retries",
        "The delay between consecutive retries in seconds.",
        "5",
        true,
        false,
        true,
    );

    let retries = number_menu(
        "number of retries",
        "The maximum number of retry attempts that will be made.",
        "5",
        false,
        true,
        false,
    );

    client_config.router_config.retry_strategy = Some(RetryStrategy::Interval { delay, retries });
}

fn retry_exponential(client_config: &mut ClientConfig) {
    let max_interval = number_menu(
        "maximum interval between requests",
        "The delay between consecutive retries in seconds.",
        "16",
        true,
        false,
        true,
    );

    let max_backoff = number_menu(
        "maximum backoff duration",
        "The maximum backoff duration that the requests will be attempted for in seconds.",
        "32",
        false,
        false,
        true,
    );

    client_config.router_config.retry_strategy = Some(RetryStrategy::Exponential {
        max_interval,
        max_backoff,
    });
}

fn retry_none(client_config: &mut ClientConfig) {
    client_config.router_config.retry_strategy = Some(RetryStrategy::None);
}

fn router_idle_timeout(client_config: &mut ClientConfig) {
    match number_menu(
        "idle timeout",
        "The maximum amount of time, in seconds, that a connection can be inactive before it is closed.",
        "60",
        true,
        false,
        true,
    ) {
        Some(value) => {
            client_config.router_config.idle_timeout = Some(value);
        }

        _ => (),
    }
}

fn router_conn_reaper_freq(client_config: &mut ClientConfig) {
    match number_menu(
        "connection reaper frequency",
        "How often, in seconds, to check for inactive connections and close them.",
        "60",
        true,
        false,
        true,
    ) {
        Some(value) => {
            client_config.router_config.conn_reaper_frequency = Some(value);
        }

        _ => (),
    }
}

fn router_buffer_size(client_config: &mut ClientConfig) {
    match number_menu(
        "buffer size",
        "The size of the router buffer for routing messages from and to remote connections.",
        "100",
        true,
        true,
        false,
    ) {
        Some(value) => {
            client_config.router_config.buffer_size = Some(value);
        }

        _ => (),
    }
}
