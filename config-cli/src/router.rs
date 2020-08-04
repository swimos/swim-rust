use crate::utils::number_menu;
use crate::{flush, get_input, print_title, show_help};

pub(crate) fn router_params_menu() {
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
            "1" => router_retry_strategy(),
            "2" => router_idle_timeout(),
            "3" => router_conn_reaper_freq(),
            "4" => router_buffer_size(),
            "b" => break,
            _ => println!("Invalid selection \"{}\"!", input),
        }
    }
}

fn router_retry_strategy() {
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
            "immediate" => {retry_immediate(); break},
            "interval" => {retry_interval(); break},
            "exponential" => {retry_exponential(); break},
            "none" => {retry_none(); break},
            _ => println!("Invalid value \"{}\" for retry strategy!", input),
        }
    }
}

fn retry_immediate() {
    //Todo
    number_menu(
        "number of retries",
        "The maximum number of retry attempts that will be made.",
        "5",
        true,
        true,
        false,
    );
}

fn retry_interval() {
    // Todo
    number_menu(
        "delay between retries",
        "The delay between consecutive retries in seconds.",
        "5",
        true,
        false,
        true,
    );

    number_menu(
        "number of retries",
        "The maximum number of retry attempts that will be made.",
        "5",
        false,
        true,
        false,
    );
}

fn retry_exponential() {
    // Todo
    number_menu(
        "maximum interval between requests",
        "The delay between consecutive retries in seconds.",
        "16",
        true,
        false,
        true,
    );

    number_menu(
        "maximum backoff duration",
        "The maximum backoff duration that the requests will be attempted for in seconds.",
        "32",
        false,
        false,
        true,
    );
}

fn retry_none() {
    // Todo
}

fn router_idle_timeout() {
    // Todo
    number_menu(
        "idle timeout",
        "The maximum amount of time, in seconds, that a connection can be inactive before it is closed.",
        "60",
        true,
        false,
        true,
    );
}

fn router_conn_reaper_freq() {
    // Todo
    number_menu(
        "connection reaper frequency",
        "How often, in seconds, to check for inactive connections and close them.",
        "60",
        true,
        false,
        true,
    );
}

fn router_buffer_size() {
    // Todo
    number_menu(
        "buffer size",
        "The size of the router buffer for routing messages from and to remote connections.",
        "100",
        true,
        true,
        false,
    );
}
