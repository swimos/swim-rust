use crate::file::save_to_file;
use crate::host_lane::{host_menu, lane_menu};
use crate::router::router_params_menu;
use crate::utils::{number_menu, BackPressure, ClientConfig, Config, DownlinkConfig, MuxMode};
use std::io::stdout;
use std::io::{stdin, Write};

mod file;
mod host_lane;
mod router;
mod utils;

fn main() {
    let mut config = Config::new();
    main_menu(&mut config);
    save_to_file(config);
}

fn get_input() -> String {
    let mut input = String::new();

    stdin()
        .read_line(&mut input)
        .expect("Error: Unable to read user input!");

    input.trim().to_owned()
}

fn flush() {
    stdout()
        .flush()
        .expect("Error: Unable to flush the buffer!");
}

fn show_help(info: &str, value: &str, default: &str) {
    print_title("Help");
    println!("{:<15} {}", "Description:", info);
    println!("{:<15} {}", "Value:", value);
    println!("{:<15} {}", "Default:", default);
}

fn print_title(text: &str) {
    println!();
    println!("--- {} ---", text);
}

fn main_menu(config: &mut Config) {
    println!("Welcome to client configuration creator.");
    println!("A helper tool for creating Recon configuration files for the Rust client.");

    loop {
        print_title("Configuration parameters");
        println!("1. Client parameters");
        println!("2. Downlink parameters");
        println!("3. Host parameters");
        println!("4. Lane parameters");
        print!("Select an option or `s` to save an exit: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "1" => client_menu(&mut config.client),
            "2" => downlinks_menu(&mut config.downlinks),
            "3" => host_menu(config),
            "4" => lane_menu(config),
            "s" => break,
            _ => println!("Invalid selection \"{}\"!", input),
        }
    }
}

fn client_menu(client_config: &mut ClientConfig) {
    loop {
        print_title("Client parameters");
        println!("1. Buffer size");
        println!("2. Router parameters");
        print!("Select an option or `b` to go back: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "1" => client_buffer_size(client_config),
            "2" => router_params_menu(client_config),
            "b" => break,
            _ => println!("Invalid selection \"{}\"!", input),
        }
    }
}

fn client_buffer_size(client_config: &mut ClientConfig) {
    match number_menu(
        "buffer size",
        "The size of the client buffer for servicing requests for new downlinks.",
        "2",
        true,
        true,
        false,
    ) {
        Some(value) => {
            client_config.buffer_size = Some(value);
        }

        _ => (),
    }
}

pub(crate) fn downlinks_menu(downlinks_config: &mut DownlinkConfig) {
    loop {
        print_title("Downlink parameters");
        println!("1. Back pressure mode");
        println!("2. Multiplexing strategy");
        println!("3. Idle timeout");
        println!("4. Buffer size");
        println!("5. Action on invalid message");
        println!("6. Yield after");
        print!("Select an option or `b` to go back: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "1" => downlinks_back_pressure(downlinks_config),
            "2" => downlinks_multiplexing_strategy(downlinks_config),
            "3" => downlinks_idle_timeout(downlinks_config),
            "4" => downlinks_buffer_size(downlinks_config),
            "5" => downlinks_on_invalid(downlinks_config),
            "6" => downlinks_yield_after(downlinks_config),
            "b" => break,
            _ => println!("Invalid selection \"{}\"!", input),
        }
    }
}

fn downlinks_back_pressure(downlinks_config: &mut DownlinkConfig) {
    loop {
        print!("Enter a value for back pressure mode, `h` for help or `b` to go back: ",);
        flush();

        let value = format!(
            "{}\n{:<15} {}",
            "* propagate - Propagate back pressure through the downlink.",
            "",
            "* release - Attempt to relieve back pressure through the downlink as much as possible."
        );

        let input = get_input();

        match input.as_str() {
            "propagate" => {
                back_pressure_propagate(downlinks_config);
                break;
            }
            "release" => {
                back_pressure_release(downlinks_config);
                break;
            }
            "h" => show_help(
                "Instruction on how the downlinks should handle back pressure.",
                &value,
                "propagate",
            ),
            "b" => break,
            _ => println!("Invalid value \"{}\" for back pressure mode!", input),
        }
    }
}

fn back_pressure_propagate(downlinks_config: &mut DownlinkConfig) {
    downlinks_config.back_pressure = Some(BackPressure::Propagate)
}

fn back_pressure_release(downlinks_config: &mut DownlinkConfig) {
    let input_buffer_size = number_menu(
        "input buffer size",
        "Input queue size for the back-pressure relief component.",
        "5",
        true,
        true,
        false,
    );
    let bridge_buffer_size = number_menu("bridge buffer size", "Queue size for control messages between different components of the pressure relief component for map downlinks.", "5", true, true, false);
    let max_active_keys = number_menu(
        "maximum active keys",
        "Maximum number of active keys in the pressure relief component for map downlinks.",
        "20",
        true,
        true,
        false,
    );
    let yield_after = number_menu(
        "yield after",
        "Number of values to process before yielding to the runtime.",
        "256",
        true,
        true,
        false,
    );

    downlinks_config.back_pressure = Some(BackPressure::Release {
        input_buffer_size,
        bridge_buffer_size,
        max_active_keys,
        yield_after,
    })
}

fn downlinks_multiplexing_strategy(downlinks_config: &mut DownlinkConfig) {
    loop {
        print!("Enter a value for multiplexing strategy, `h` for help or `b` to go back: ",);
        flush();

        let value = format!(
            "{}\n{:<15} {}\n{:<15} {}",
            "* queue - Each consumer has an intermediate queues. If any one of these queues fills the downlink will block.",
            "",
            "* dropping - Each subscriber to the downlink will see only the most recent event each time it polls.",
            "",
            "* buffered - All consumers read from a single intermediate queue. If this queue fills the oldest values will be discarded."
        );

        let input = get_input();

        match input.as_str() {
            "queue" => {
                mux_strat_queue(downlinks_config);
                break;
            }
            "dropping" => {
                mux_strat_dropping(downlinks_config);
                break;
            }
            "buffered" => {
                mux_strat_buffered(downlinks_config);
                break;
            }
            "h" => show_help(
                "Multiplexing strategy for the topic of events produced by a downlink.",
                &value,
                "queue",
            ),
            "b" => break,
            _ => println!("Invalid value \"{}\" for multiplexing strategy!", input),
        }
    }
}

fn mux_strat_queue(downlinks_config: &mut DownlinkConfig) {
    let queue_size = number_menu(
        "queue size",
        "Size of the intermediate queues.",
        "5",
        true,
        true,
        false,
    );

    downlinks_config.mux_mode = Some(MuxMode::Queue { queue_size });
}

fn mux_strat_dropping(downlinks_config: &mut DownlinkConfig) {
    downlinks_config.mux_mode = Some(MuxMode::Dropping)
}

fn mux_strat_buffered(downlinks_config: &mut DownlinkConfig) {
    let queue_size = number_menu(
        "queue size",
        "Size of the intermediate queue.",
        "5",
        true,
        true,
        false,
    );

    downlinks_config.mux_mode = Some(MuxMode::Buffered { queue_size });
}

fn downlinks_idle_timeout(downlinks_config: &mut DownlinkConfig) {
    match number_menu(
        "idle timeout",
        "Timeout, in seconds, after which an idle downlink will be closed.",
        "60000",
        true,
        false,
        true,
    ) {
        Some(value) => downlinks_config.idle_timeout = Some(value),
        None => (),
    }
}

fn downlinks_buffer_size(downlinks_config: &mut DownlinkConfig) {
    match number_menu(
        "buffer size",
        "Buffer size for local actions performed on the downlinks.",
        "5",
        true,
        true,
        false,
    ) {
        Some(value) => downlinks_config.buffer_size = Some(value),
        None => (),
    }
}

fn downlinks_on_invalid(downlinks_config: &mut DownlinkConfig) {
    loop {
        print!("Enter a value for action on invalid message, `h` for help or `b` to go back: ",);
        flush();

        let value = format!(
            "{}\n{:<15} {}",
            "* ignore - Disregard the message and continue.",
            "",
            "* terminate - Terminate the downlink."
        );

        let input = get_input();

        match input.as_str() {
            "terminate" => {
                downlinks_config.on_invalid = Some(format!("\"{}\"", input));
                break;
            }
            "ignore" => {
                downlinks_config.on_invalid = Some(format!("\"{}\"", input));
                break;
            }
            "h" => show_help(
                "Instruction on how to respond when an invalid message is received for a downlink.",
                &value,
                "terminate",
            ),
            "b" => break,
            _ => println!("Invalid value \"{}\" for action on invalid message!", input),
        }
    }
}

fn downlinks_yield_after(downlinks_config: &mut DownlinkConfig) {
    match number_menu(
        "yield after",
        "Number of operations after which a downlink will yield to the runtime..",
        "256",
        true,
        true,
        false,
    ) {
        Some(value) => downlinks_config.yield_after = Some(value),
        None => (),
    }
}
