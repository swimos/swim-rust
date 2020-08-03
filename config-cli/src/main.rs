use crate::router::router_params_menu;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::stdout;
use std::io::{stdin, Write};
use std::num::ParseIntError;

mod router;

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
    println!();
    println!("{}", info);
    println!("Value: {}", value);
    println!("Default: {}", default);
}

fn main_menu() {
    loop {
        println!();
        println!("Configuration parameters");
        println!("1. Client parameters");
        println!("2. Downlinks parameters");
        println!("3. Host parameters");
        println!("4. Lane parameters");
        print!("Select an option or `s` to save an exit: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "1" => client_menu(),
            "2" => println!("{}", input),
            "3" => println!("{}", input),
            "4" => println!("{}", input),
            //Todo
            "s" => break,
            _ => println!("Invalid selection \"{}\"!", input),
        }
    }
}

fn client_menu() {
    loop {
        println!();
        println!("Client parameters");
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
    loop {
        print!("Enter a value for buffer size, `h` for help or `b` to go back: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "h" => show_help(
                "The size of the client buffer for servicing requests for new downlinks.",
                "Integer greater than 0",
                "2",
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

fn downlinks_menu() {

    println!();
    println!("Downlinks parameters");

}
