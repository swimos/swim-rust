use crate::{flush, get_input, show_help};
use std::collections::HashMap;

const INDEFINITE_VALUE: &str = "\"indefinite\"";

pub(crate) fn number_menu(
    name: &str,
    info: &str,
    default: &str,
    finite: bool,
    non_zero: bool,
    duration: bool,
) -> Option<String> {
    let value = if non_zero {
        if finite {
            "Positive integer"
        } else {
            "Positive integer or \"indefinite\""
        }
    } else {
        if finite {
            "Non-negative integer"
        } else {
            "Non-negative integer or \"indefinite\""
        }
    };

    loop {
        if duration {
            print!(
                "Enter a value for {}, in seconds, `h` for help or `b` to go back: ",
                name
            );
        } else {
            print!(
                "Enter a value for {}, `h` for help or `b` to go back: ",
                name
            );
        }

        flush();

        let input = get_input();

        match input.as_str() {
            "h" => show_help(info, value, default),
            "b" => return None,
            _ => {
                if input == "indefinite" && !finite {
                    return Some(INDEFINITE_VALUE.to_owned());
                } else {
                    match input.parse::<usize>() {
                        Ok(value) if value > 0 || !non_zero => return Some(value.to_string()),
                        _ => println!("Invalid value \"{}\" for {}!", input, name),
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) client: ClientConfig,
    pub(crate) downlinks: DownlinkConfig,
    pub(crate) hosts: HashMap<String, DownlinkConfig>,
    pub(crate) lanes: HashMap<LaneConfig, DownlinkConfig>,
}

impl Config {
    pub(crate) fn new() -> Self {
        Config {
            client: ClientConfig::new(),
            downlinks: DownlinkConfig::new(),
            hosts: HashMap::new(),
            lanes: HashMap::new(),
        }
    }

    pub(crate) fn save_to_file(self) {
        // Todo
        println!("{:?}", self);
    }
}

#[derive(Debug)]
pub(crate) struct ClientConfig {
    pub(crate) buffer_size: Option<String>,
    pub(crate) router_config: RouterConfig,
}

impl ClientConfig {
    pub(crate) fn new() -> Self {
        ClientConfig {
            buffer_size: None,
            router_config: RouterConfig::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RouterConfig {
    pub(crate) retry_strategy: Option<RetryStrategy>,
    pub(crate) idle_timeout: Option<String>,
    pub(crate) conn_reaper_frequency: Option<String>,
    pub(crate) buffer_size: Option<String>,
}

impl RouterConfig {
    pub(crate) fn new() -> Self {
        RouterConfig {
            retry_strategy: None,
            idle_timeout: None,
            conn_reaper_frequency: None,
            buffer_size: None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum RetryStrategy {
    Immediate {
        retries: Option<String>,
    },
    Interval {
        delay: Option<String>,
        retries: Option<String>,
    },
    Exponential {
        max_interval: Option<String>,
        max_backoff: Option<String>,
    },
    None,
}

#[derive(Debug)]
pub(crate) struct DownlinkConfig {
    pub(crate) back_pressure: Option<BackPressure>,
    pub(crate) mux_mode: Option<MuxMode>,
    pub(crate) idle_timeout: Option<String>,
    pub(crate) buffer_size: Option<String>,
    pub(crate) on_invalid: Option<String>,
    pub(crate) yield_after: Option<String>,
}

impl DownlinkConfig {
    pub(crate) fn new() -> Self {
        DownlinkConfig {
            back_pressure: None,
            mux_mode: None,
            idle_timeout: None,
            buffer_size: None,
            on_invalid: None,
            yield_after: None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum BackPressure {
    Propagate,
    Release {
        input_buffer_size: Option<String>,
        bridge_buffer_size: Option<String>,
        max_active_keys: Option<String>,
        yield_after: Option<String>,
    },
}

#[derive(Debug)]
pub(crate) enum MuxMode {
    Queue { queue_size: Option<String> },
    Dropping,
    Buffered { queue_size: Option<String> },
}

#[derive(Debug)]
pub(crate) struct LaneConfig {
    host: String,
    node: String,
    lane: String,
}
