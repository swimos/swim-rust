use crate::host_lane::{host_menu, lane_menu};
use crate::router::router_params_menu;
use crate::utils::number_menu;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::stdout;
use std::io::{stdin, Write};
use std::num::ParseIntError;

mod host_lane;
mod router;
mod utils;

const CLIENT_BUFFER_SIZE: &str = "client_buffer_size";

fn main() {
    // let config = HashMap::new();
    main_menu();
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

fn main_menu() {
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
            "1" => client_menu(),
            "2" => downlinks_menu(),
            "3" => host_menu(),
            "4" => lane_menu(),
            "s" => break,
            _ => println!("Invalid selection \"{}\"!", input),
        }
    }
}

fn client_menu() {
    loop {
        print_title("Client parameters");
        println!("1. Buffer size");
        println!("2. Router parameters");
        print!("Select an option or `b` to go back: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "1" => client_buffer_size(),
            "2" => router_params_menu(),
            "b" => break,
            _ => println!("Invalid selection \"{}\"!", input),
        }
    }
}

fn client_buffer_size() {
    number_menu(
        "buffer size",
        "The size of the client buffer for servicing requests for new downlinks.",
        "2",
        true,
        true,
        false,
    );
}

pub(crate) fn downlinks_menu() {
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
            "1" => downlinks_back_pressure(),
            "2" => downlinks_multiplexing_strategy(),
            "3" => downlinks_idle_timeout(),
            "4" => downlinks_buffer_size(),
            "5" => downlinks_on_invalid(),
            "6" => downlinks_yield_after(),
            "b" => break,
            _ => println!("Invalid selection \"{}\"!", input),
        }
    }
}

fn downlinks_back_pressure() {
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
                back_pressure_propagate();
                break;
            }
            "release" => {
                back_pressure_release();
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

fn back_pressure_propagate() {
    //Todo
}

fn back_pressure_release() {
    //Todo
    number_menu(
        "input buffer size",
        "Input queue size for the back-pressure relief component.",
        "5",
        true,
        true,
        false,
    );
    number_menu("bridge buffer size", "Queue size for control messages between different components of the pressure relief component for map downlinks.", "5", true, true, false);
    number_menu(
        "maximum active keys",
        "Maximum number of active keys in the pressure relief component for map downlinks.",
        "20",
        true,
        true,
        false,
    );
    number_menu(
        "yield after",
        "Number of values to process before yielding to the runtime.",
        "256",
        true,
        true,
        false,
    );
}

fn downlinks_multiplexing_strategy() {
    //Todo
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
                mux_strat_queue();
                break;
            }
            "dropping" => {
                mux_strat_dropping();
                break;
            }
            "buffered" => {
                mux_strat_buffered();
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

fn mux_strat_queue() {
    //Todo
    number_menu(
        "queue size",
        "Size of the intermediate queues.",
        "5",
        true,
        true,
        false,
    );
}

fn mux_strat_dropping() {
    //Todo
}

fn mux_strat_buffered() {
    //Todo
    number_menu(
        "queue size",
        "Size of the intermediate queue.",
        "5",
        true,
        true,
        false,
    );
}

fn downlinks_idle_timeout() {
    // Todo
    number_menu(
        "idle timeout",
        "Timeout, in seconds, after which an idle downlink will be closed.",
        "60000",
        true,
        false,
        true,
    );
}

fn downlinks_buffer_size() {
    // Todo
    number_menu(
        "buffer size",
        "Buffer size for local actions performed on the downlinks.",
        "5",
        true,
        true,
        false,
    );
}

fn downlinks_on_invalid() {
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
                //Todo
                break;
            }
            "ignore" => {
                //Todo
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

fn downlinks_yield_after() {
    // Todo
    number_menu(
        "yield after",
        "Number of operations after which a downlink will yield to the runtime..",
        "256",
        true,
        true,
        false,
    );
}
