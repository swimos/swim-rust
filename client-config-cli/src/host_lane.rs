use crate::utils::{text_menu, url_menu, Config, DownlinkConfig, LanePath};
use crate::{downlinks_menu, flush, get_input, print_title};

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
            println!("{}", url);
        }
    } else {
        println!("No hosts with custom parameters have been created!")
    }
}

fn host_url(new: bool, config: &mut Config) -> Option<String> {
    loop {
        let url = url_menu()?;

        if new {
            if !config.hosts.contains_key(&url) {
                return Some(url);
            } else {
                println!("A host with URI \"{}\" already exists!", url);
            }
        } else {
            if config.hosts.contains_key(&url) {
                return Some(url);
            } else {
                println!("No exiting host with URI \"{}\"!", url);
            }
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
    let maybe_path = lane_path(true, config);

    if let Some(path) = maybe_path {
        let mut downlink_config = DownlinkConfig::new();
        downlinks_menu(&mut downlink_config);
        config.lanes.insert(path, downlink_config);
    }
}

fn edit_lane(config: &mut Config) {
    let maybe_path = lane_path(false, config);

    if let Some(path) = maybe_path {
        let mut downlink_config = config.lanes.get_mut(&path).unwrap();
        downlinks_menu(&mut downlink_config);
    }
}

fn remove_lane(config: &mut Config) {
    let maybe_path = lane_path(false, config);

    if let Some(path) = maybe_path {
        let _ = config.lanes.remove(&path).unwrap();
    }
}

fn show_all_lanes(config: &mut Config) {
    if config.lanes.len() > 0 {
        print_title("Lanes with parameters");
        for (path, _) in &config.lanes {
            println!("{}", path);
        }
    } else {
        println!("No lanes with custom parameters have been created!")
    }
}

fn lane_path(new: bool, config: &mut Config) -> Option<LanePath> {
    loop {
        let host = url_menu()?;
        let node = text_menu(
            "node",
            "Name of the node of the remote agent.",
            "Non-empty string",
            "foo",
        )?;
        let lane = text_menu(
            "lane",
            "Name of the lane of the remote agent.",
            "Non-empty string",
            "bar",
        )?;

        let path = LanePath { host, node, lane };

        if new {
            if !config.lanes.contains_key(&path) {
                return Some(path);
            } else {
                println!("A lane with path \"{}\" already exists!", path);
            }
        } else {
            if config.lanes.contains_key(&path) {
                return Some(path);
            } else {
                println!("No exiting lane with path \"{}\"!", path);
            }
        }
    }
}
