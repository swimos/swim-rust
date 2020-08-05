use crate::utils::{Config, DownlinkConfig};
use crate::{downlinks_menu, flush, get_input, print_title, show_help};

pub(crate) fn host_menu(config: &mut Config) {
    loop {
        print_title("Host parameters");
        println!("1. Add new host parameters");
        println!("2. Edit exiting host parameters");
        println!("3. Remove host parameters");
        println!("4. Show all hosts with custom parameters");
        print!("Select an option or `b` to go back: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "1" => add_host_url(config),
            "2" => edit_existing_host(config),
            "3" => remove_host(config),
            "4" => show_all_hosts(config),
            "b" => break,
            _ => println!("Invalid selection \"{}\"!", input),
        }
    }
}

fn add_host_url(config: &mut Config) {
    let maybe_url = host_url(true, config);

    if let Some(url) = maybe_url {
        let mut downlink_config = DownlinkConfig::new();
        downlinks_menu(&mut downlink_config);
        config.hosts.insert(url, downlink_config);
    }
}

fn edit_existing_host(config: &mut Config) {
    let maybe_url = host_url(false, config);

    if let Some(url) = maybe_url {
        let mut downlink_config = config.hosts.get_mut(&url).unwrap();
        downlinks_menu(&mut downlink_config);
    }
}

fn remove_host(config: &mut Config) {
    let maybe_url = host_url(false, config);

    if let Some(url) = maybe_url {
        let _ = config.hosts.remove(&url).unwrap();
    }
}

fn show_all_hosts(config: &mut Config) {
    if config.hosts.len() > 0 {
        print_title("Hosts with parameters");
        for (url, _) in &config.hosts {
            println!("{:?}", url);
        }
    } else {
        println!("No hosts with custom parameters have been created!")
    }
}

fn host_url(new: bool, config: &mut Config) -> Option<String> {
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
            "b" => return None,
            _ => match url::Url::parse(&input) {
                Ok(url) if new => {
                    let url_str = url.to_string();

                    if !config.hosts.contains_key(&url_str) {
                        return Some(url_str);
                    } else {
                        println!("A host with URI \"{}\" already exists!", input);
                    }
                }
                Ok(url) if !new => {
                    let url_str = url.to_string();

                    if config.hosts.contains_key(&url_str) {
                        return Some(url_str);
                    } else {
                        println!("No exiting host with URI \"{}\"!", input);
                    }
                }
                _ => println!("Invalid value \"{}\" for host URI!", input),
            },
        }
    }
}

pub(crate) fn lane_menu(config: &mut Config) {
    loop {
        print_title("Lane parameters");
        println!("1. Add new lane parameters");
        println!("2. Edit exiting lane parameters");
        println!("3. Remove lane parameters");
        println!("4. Show all lanes with custom parameters");
        print!("Select an option or `b` to go back: ");
        flush();

        let input = get_input();

        match input.as_str() {
            "1" => add_lane(config),
            "2" => edit_lane(config),
            "3" => remove_lane(config),
            "4" => show_all_lanes(config),
            "b" => break,
            _ => println!("Invalid selection \"{}\"!", input),
        }
    }
}

fn add_lane(config: &mut Config) {
    //Todo
    // let maybe_url = host_url(true, config);
    //
    // if let Some(url) = maybe_url {
    //     let mut downlink_config = DownlinkConfig::new();
    //     downlinks_menu(&mut downlink_config);
    //     config.hosts.insert(url, downlink_config);
    // }
}

fn edit_lane(config: &mut Config) {
    //Todo
    // let maybe_url = host_url(false, config);
    //
    // if let Some(url) = maybe_url {
    //     let mut downlink_config = config.hosts.get_mut(&url).unwrap();
    //     downlinks_menu(&mut downlink_config);
    // }
}

fn remove_lane(config: &mut Config) {
    //Todo
    // let maybe_url = host_url(false, config);
    //
    // if let Some(url) = maybe_url {
    //     let _ = config.hosts.remove(&url).unwrap();
    // }
}

fn show_all_lanes(config: &mut Config) {
    if config.lanes.len() > 0 {
        print_title("Lanes with parameters");
        for (url, _) in &config.hosts {
            println!("{:?}", url);
        }
    } else {
        println!("No lanes with custom parameters have been created!")
    }
}
