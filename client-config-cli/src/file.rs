use crate::utils::{
    BackPressure, ClientConfig, Config, DownlinkConfig, LanePath, MuxMode, RetryStrategy,
    RouterConfig,
};
use crate::{flush, get_input};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;

const INDENT_NUM: usize = 4;

const CONFIG_TAG: &str = "config";

const CLIENT_TAG: &str = "client";
const BUFFER_SIZE_TAG: &str = "buffer_size";
const ROUTER_TAG: &str = "router";
const RETRY_STRAT_TAG: &str = "retry_strategy";
const IDLE_TIMEOUT_TAG: &str = "idle_timeout";
const CONNECTION_REAPER_FREQ_TAG: &str = "conn_reaper_frequency";
const IMMEDIATE_TAG: &str = "immediate";
const INTERVAL_TAG: &str = "interval";
const EXPONENTIAL_TAG: &str = "exponential";
const NONE_TAG: &str = "none";
const RETRIES_TAG: &str = "retries";
const DELAY_TAG: &str = "delay";
const MAX_INTERVAL_TAG: &str = "max_interval";
const MAX_BACKOFF_TAG: &str = "max_backoff";

const DOWNLINKS_TAG: &str = "downlinks";
const BACK_PRESSURE_TAG: &str = "back_pressure";
const PROPAGATE_TAG: &str = "propagate";
const RELEASE_TAG: &str = "release";
const INPUT_BUFFER_SIZE_TAG: &str = "input_buffer_size";
const BRIDGE_BUFFER_SIZE_TAG: &str = "bridge_buffer_size";
const MAX_ACTIVE_KEYS_TAG: &str = "max_active_keys";
const YIELD_AFTER_TAG: &str = "yield_after";
const MUX_MODE_TAG: &str = "mux_mode";
const QUEUE_TAG: &str = "queue";
const QUEUE_SIZE_TAG: &str = "queue_size";
const DROPPING_TAG: &str = "dropping";
const BUFFERED_TAG: &str = "buffered";
const ON_INVALID_TAG: &str = "on_invalid";

const HOST_TAG: &str = "host";
const LANE_TAG: &str = "lane";
const PATH_TAG: &str = "path";
const NODE_TAG: &str = "node";

pub(crate) fn save_to_file(config: Config) {
    print!("Enter a filename: ");
    flush();

    let input = get_input();
    let mut file = File::create(format!("{}.recon", input)).expect("Invalid file name!");

    let text_config = config_to_text(config);
    file.write_all(text_config.as_bytes()).unwrap();
}

fn config_to_text(config: Config) -> String {
    let Config {
        client,
        downlinks,
        hosts,
        lanes,
    } = config;

    write_block(
        CONFIG_TAG,
        vec![
            client_to_text(client),
            downlinks_to_text(DOWNLINKS_TAG, 2, true, downlinks),
            hosts_to_text(hosts),
            lanes_to_text(lanes),
        ],
        1,
        true,
    )
}

fn client_to_text(client_config: ClientConfig) -> Option<String> {
    let ClientConfig {
        buffer_size,
        router_config,
    } = client_config;

    Some(write_block(
        CLIENT_TAG,
        vec![
            write_param(BUFFER_SIZE_TAG, buffer_size),
            router_to_text(router_config),
        ],
        2,
        true,
    ))
}

fn router_to_text(router_config: RouterConfig) -> Option<String> {
    let RouterConfig {
        retry_strategy,
        idle_timeout,
        conn_reaper_frequency,
        buffer_size,
    } = router_config;

    Some(write_block(
        ROUTER_TAG,
        vec![
            write_param(RETRY_STRAT_TAG, retry_strat_to_text(retry_strategy)),
            write_param(IDLE_TIMEOUT_TAG, idle_timeout),
            write_param(CONNECTION_REAPER_FREQ_TAG, conn_reaper_frequency),
            write_param(BUFFER_SIZE_TAG, buffer_size),
        ],
        3,
        false,
    ))
}

fn retry_strat_to_text(retry_config: Option<RetryStrategy>) -> Option<String> {
    Some(match retry_config? {
        RetryStrategy::Immediate { retries } => {
            write_rec(IMMEDIATE_TAG, vec![write_param(RETRIES_TAG, retries)], true)
        }
        RetryStrategy::Interval { delay, retries } => write_rec(
            INTERVAL_TAG,
            vec![
                write_param(DELAY_TAG, delay),
                write_param(RETRIES_TAG, retries),
            ],
            true,
        ),
        RetryStrategy::Exponential {
            max_interval,
            max_backoff,
        } => write_rec(
            EXPONENTIAL_TAG,
            vec![
                write_param(MAX_INTERVAL_TAG, max_interval),
                write_param(MAX_BACKOFF_TAG, max_backoff),
            ],
            true,
        ),
        RetryStrategy::None => write_rec(NONE_TAG, vec![], true),
    })
}

fn downlinks_to_text(
    name: &str,
    level: usize,
    attr: bool,
    downlink_config: DownlinkConfig,
) -> Option<String> {
    let DownlinkConfig {
        back_pressure,
        mux_mode,
        idle_timeout,
        buffer_size,
        on_invalid,
        yield_after,
    } = downlink_config;

    Some(write_block(
        name,
        vec![
            write_param(BACK_PRESSURE_TAG, back_pressure_to_text(back_pressure)),
            write_param(MUX_MODE_TAG, mux_mode_to_text(mux_mode)),
            write_param(IDLE_TIMEOUT_TAG, idle_timeout),
            write_param(BUFFER_SIZE_TAG, buffer_size),
            write_param(ON_INVALID_TAG, on_invalid),
            write_param(YIELD_AFTER_TAG, yield_after),
        ],
        level,
        attr,
    ))
}

fn back_pressure_to_text(back_pressure_config: Option<BackPressure>) -> Option<String> {
    Some(match back_pressure_config? {
        BackPressure::Propagate => write_rec(PROPAGATE_TAG, vec![], true),
        BackPressure::Release {
            input_buffer_size,
            bridge_buffer_size,
            max_active_keys,
            yield_after,
        } => write_rec(
            RELEASE_TAG,
            vec![
                write_param(INPUT_BUFFER_SIZE_TAG, input_buffer_size),
                write_param(BRIDGE_BUFFER_SIZE_TAG, bridge_buffer_size),
                write_param(MAX_ACTIVE_KEYS_TAG, max_active_keys),
                write_param(YIELD_AFTER_TAG, yield_after),
            ],
            true,
        ),
    })
}

fn mux_mode_to_text(mux_mode_config: Option<MuxMode>) -> Option<String> {
    Some(match mux_mode_config? {
        MuxMode::Queue { queue_size } => write_rec(
            QUEUE_TAG,
            vec![write_param(QUEUE_SIZE_TAG, queue_size)],
            true,
        ),
        MuxMode::Dropping => write_rec(DROPPING_TAG, vec![], true),
        MuxMode::Buffered { queue_size } => write_rec(
            BUFFERED_TAG,
            vec![write_param(QUEUE_SIZE_TAG, queue_size)],
            true,
        ),
    })
}

fn hosts_to_text(hosts_map: HashMap<String, DownlinkConfig>) -> Option<String> {
    if hosts_map.is_empty() {
        None
    } else {
        let host_configs = hosts_map
            .into_iter()
            .map(|(host, config)| downlinks_to_text(&host, 3, false, config))
            .collect();

        Some(write_block(HOST_TAG, host_configs, 2, true))
    }
}

fn lanes_to_text(lanes_map: HashMap<LanePath, DownlinkConfig>) -> Option<String> {
    if lanes_map.is_empty() {
        None
    } else {
        let lane_configs = lanes_map
            .into_iter()
            .map(|(LanePath { host, node, lane }, config)| {
                downlinks_to_text(
                    &write_rec(
                        PATH_TAG,
                        vec![
                            write_param(HOST_TAG, Some(host)),
                            write_param(NODE_TAG, Some(node)),
                            write_param(LANE_TAG, Some(lane)),
                        ],
                        false,
                    ),
                    3,
                    true,
                    config,
                )
            })
            .collect();

        Some(write_block(LANE_TAG, lane_configs, 2, true))
    }
}

fn write_block(name: &str, content: Vec<Option<String>>, level: usize, attr: bool) -> String {
    let mut block = String::new();

    if attr {
        block.push_str(&format!("@{} {}\n", name, "{",));
    } else {
        block.push_str(&format!("{}: {}\n", name, "{",));
    }

    let content: Vec<String> = content.into_iter().flatten().collect();

    if !content.is_empty() {
        for line_num in 0..content.len() - 1 {
            block.push_str(&format!(
                "{:indent$}{},\n",
                "",
                content.get(line_num).unwrap(),
                indent = level * INDENT_NUM
            ));
        }

        block.push_str(&format!(
            "{:indent$}{}\n",
            "",
            content.get(content.len() - 1).unwrap(),
            indent = level * INDENT_NUM
        ));
    }

    block.push_str(&format!(
        "{:indent$}{}",
        "",
        "}",
        indent = (level - 1) * INDENT_NUM
    ));

    block
}

fn write_rec(name: &str, content: Vec<Option<String>>, attr: bool) -> String {
    let mut rec = String::new();

    if attr {
        rec.push_str(&format!("@{}(", name));
    } else {
        rec.push_str(&format!("{}(", name))
    }

    let content: Vec<String> = content
        .into_iter()
        .filter(|c| c.is_some())
        .map(|c| c.unwrap())
        .collect();

    if !content.is_empty() {
        for line_num in 0..content.len() - 1 {
            rec.push_str(&format!("{}, ", content.get(line_num).unwrap()))
        }

        rec.push_str(&format!("{}", content.get(content.len() - 1).unwrap()));
    }

    rec.push_str(")");
    rec
}

fn write_param(key: &str, value: Option<String>) -> Option<String> {
    Some(format!("{}: {}", key, value?))
}
