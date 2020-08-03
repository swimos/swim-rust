use crate::{flush, get_input, show_help};

pub(crate) fn router_params_menu() {
    loop {
        println!();
        println!("Router parameters");
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

        match input.as_str() {
            "h" => show_help(
                "The retry strategy that will be used when attempting to make a request to a remote agent if a failure occurs.",
                "\n * immediate - A retry strategy that does an immediate retry with no sleep time in between the requests.\n \
                        * interval - A retry strategy with a defined delay in between the requests.\n \
                        * exponential - A truncated exponential retry strategy.\n \
                        * none - A retry strategy that only attempts the request once.",
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
    retry_number(true);
}

fn retry_interval() {
    // Todo
    retry_duration(
        "delay between retries",
        "The delay between consecutive retries in seconds.",
        "5",
        true,
    );
    retry_number(false);
}

fn retry_exponential() {
    // Todo
    retry_duration(
        "maximum interval between requests",
        "The delay between consecutive retries in seconds.",
        "16",
        true,
    );

    retry_duration(
        "maximum backoff duration",
        "The maximum backoff duration that the requests will be attempted for in seconds.",
        "32",
        false,
    );
}

fn retry_none() {
    // Todo
}

fn retry_number(finite: bool) {
    let value = if finite {
        "Integer greater than 0"
    } else {
        "Integer greater than 0 or \"indefinite\""
    };

    loop {
        print!("Enter a value for number of retries, `h` for help or `b` to go back: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "h" => show_help(
                "The maximum number of retry attempts that will be made.",
                value,
                "5",
            ),
            "b" => break,
            _ => {
                if input == "indefinite" && !finite {
                    //Todo
                    break;
                } else {
                    match input.parse::<usize>() {
                        //Todo
                        Ok(value) if value > 0 => break,
                        _ => println!("Invalid value \"{}\" for number of retries!", input),
                    }
                }
            }
        }
    }
}

fn retry_duration(name: &str, info: &str, default: &str, finite: bool) {
    let value = if finite {
        "Positive integer"
    } else {
        "Positive integer or \"indefinite\""
    };

    loop {
        print!(
            "Enter a value for {} in seconds, `h` for help or `b` to go back: ",
            name
        );
        flush();

        let input = get_input();

        match input.as_str() {
            "h" => show_help(info, value, default),
            "b" => break,
            _ => {
                if input == "indefinite" && !finite {
                    //Todo
                    break;
                } else {
                    match input.parse::<usize>() {
                        //Todo
                        Ok(value) => break,
                        _ => println!("Invalid value \"{}\" for delay!", input),
                    }
                }
            }
        }
    }
}

fn router_idle_timeout() {
    loop {
        print!("Enter a value for idle timeout, `h` for help or `b` to go back: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "h" => show_help(
                "The maximum amount of time, in seconds, that a connection can be inactive before it is closed.",
                "Positive integer",
                "60",
            ),
            "b" => break,
            _ => match input.parse::<usize>() {
                //Todo
                Ok(value) => break,
                _ => println!("Invalid value \"{}\" for idle timeout!", input),
            },
        }
    }
}

fn router_conn_reaper_freq() {
    loop {
        print!("Enter a value for connection reaper frequency, `h` for help or `b` to go back: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "h" => show_help(
                "How often, in seconds, to check for inactive connections and close them.",
                "Positive integer",
                "60",
            ),
            "b" => break,
            _ => match input.parse::<usize>() {
                //Todo
                Ok(value) => break,
                _ => println!(
                    "Invalid value \"{}\" for connection reaper frequency!",
                    input
                ),
            },
        }
    }
}

fn router_buffer_size() {
    loop {
        print!("Enter a value for buffer size, `h` for help or `b` to go back: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "h" => show_help(
                "The size of the router buffer for routing messages from and to remote connections.",
                "Integer greater than 0",
                "100",
            ),
            "b" => break,
            _ => match input.parse::<usize>() {
                //Todo
                Ok(value) if value > 0 => break,
                _ => println!("Invalid value \"{}\" for buffer size!", input),
            },
        }
    }
}
