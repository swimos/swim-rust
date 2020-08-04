use crate::{downlinks_menu, flush, get_input, print_title, show_help};

pub(crate) fn host_menu() {
    loop {
        print_title("Host parameters");
        println!("1. Add new host parameters");
        println!("2. Edit exiting host parameters");
        println!("3. Remove host parameters");
        println!("4. Show all hosts");
        print!("Select an option or `b` to go back: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "1" => add_host_url(),
            "2" => edit_existing_host(),
            "3" => remove_host(),
            "4" => show_all_hosts(),
            "b" => break,
            _ => println!("Invalid selection \"{}\"!", input),
        }
    }
}

fn add_host_url() {
    //Todo
    host_url(true);
    downlinks_menu();
}

fn edit_existing_host() {
    //Todo
    host_url(false);
    downlinks_menu();
}

fn remove_host() {
    //Todo
    host_url(false);
}

fn show_all_hosts() {
    //Todo
}

fn host_url(new: bool) {
    loop {
        print!("Enter a value for host URI, `h` for help or `b` to go back: ",);
        flush();

        let input = get_input();

        match input.as_str() {
            "h" => show_help(
                "An absolute URI for a remote host. The URI must include the scheme.",
                "Absolute URI",
                "ws://127.0.0.1/",
            ),
            "b" => break,
            _ => match url::Url::parse(&input) {
                Ok(url) if new => {
                    //Todo,
                    break;
                }
                Ok(url) if !new => {
                    //Todo,
                    println!("No exiting host with URI \"{}\"!", input);
                }
                _ => println!("Invalid value \"{}\" for host URI!", input),
            },
        }
    }
}

pub(crate) fn lane_menu() {}
