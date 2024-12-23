// Copyright 2015-2024 Swim Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;

use console_views::history::HistoryEditView;
use cursive::theme::Color::{self, TerminalDefault};
use cursive::theme::PaletteColor::*;
use cursive::utils::markup::StyledString;
use cursive::{
    theme::{BaseColor, BorderStyle, Palette, Theme},
    view::{Nameable, Resizable, ScrollStrategy, Scrollable},
    views::{LinearLayout, Panel, TextView},
    Cursive, With,
};
use std::time::Duration;

use crate::model::{parse_app_command, AppCommand, DisplayResponse, LogMessageKind};
use crate::{controller::Controller, model::UIUpdate};

use self::bounded_append::BoundedAppend;

mod bounded_append;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UIFailed {
    Stopped,
    TimedOut,
}

impl<T> From<crossbeam_channel::SendError<T>> for UIFailed {
    fn from(_: crossbeam_channel::SendError<T>) -> Self {
        UIFailed::Stopped
    }
}

impl<T> From<crossbeam_channel::SendTimeoutError<T>> for UIFailed {
    fn from(e: crossbeam_channel::SendTimeoutError<T>) -> Self {
        match e {
            crossbeam_channel::SendTimeoutError::Timeout(_) => UIFailed::TimedOut,
            crossbeam_channel::SendTimeoutError::Disconnected(_) => UIFailed::Stopped,
        }
    }
}

pub trait ViewUpdater {
    fn update(&self, upd: UIUpdate) -> Result<(), UIFailed>;
}

pub struct CursiveUIUpdater {
    sink: cursive::CbSink,
    timeout: Duration,
    link_appender: BoundedAppend<&'static str, DisplayResponse>,
    log_appender: BoundedAppend<&'static str, (LogMessageKind, String)>,
}

impl CursiveUIUpdater {
    pub fn new(sink: cursive::CbSink, timeout: Duration, max_lines: usize) -> Self {
        CursiveUIUpdater {
            sink,
            timeout,
            link_appender: BoundedAppend::new(LINKS_VIEW, max_lines, format_display),
            log_appender: BoundedAppend::new(LOG_VIEW, max_lines, format_log_line),
        }
    }
}

const COLOURS: &[Color] = &[
    Color::Dark(BaseColor::Blue),
    Color::Light(BaseColor::Blue),
    Color::Light(BaseColor::Green),
    Color::Dark(BaseColor::Yellow),
];

impl ViewUpdater for CursiveUIUpdater {
    fn update(&self, update: UIUpdate) -> Result<(), UIFailed> {
        let CursiveUIUpdater {
            sink,
            timeout,
            link_appender,
            log_appender,
        } = self;
        match update {
            UIUpdate::LinkDisplay(display) => {
                let link_appender = *link_appender;
                sink.send_timeout(
                    Box::new(move |s| {
                        link_appender.append(s, display);
                    }),
                    *timeout,
                )?;
            }
            UIUpdate::LogMessage(kind, msg) => {
                let log_appender = *log_appender;
                sink.send_timeout(
                    Box::new(move |s| {
                        log_appender.append(s, (kind, msg));
                    }),
                    *timeout,
                )?;
            }
        }
        Ok(())
    }
}

fn format_log_line(input: (LogMessageKind, String)) -> StyledString {
    let (kind, message) = input;
    let colour = match kind {
        LogMessageKind::Report => BaseColor::Cyan.dark(),
        LogMessageKind::Data => BaseColor::White.dark(),
        LogMessageKind::Error => BaseColor::Red.light(),
    };
    StyledString::styled(format!("{}\n", message), colour)
}

const HISTORY_VIEW: &str = "history";
const LINKS_VIEW: &str = "links";
const LOG_VIEW: &str = "log";

const COMMAND_EDIT: &str = "command";
const HISTORY_LEN: usize = 32;

pub fn create_ui(siv: &mut Cursive, mut controller: Controller, max_lines: usize) {
    siv.add_global_callback('q', |s| s.quit());

    siv.set_theme(Theme {
        shadow: false,
        borders: BorderStyle::Outset,
        palette: Palette::default().with(|palette| {
            palette[Background] = TerminalDefault;
            palette[View] = TerminalDefault;
            palette[Primary] = BaseColor::White.dark();
            palette[TitlePrimary] = BaseColor::Blue.light();
            palette[Secondary] = BaseColor::White.dark();
            palette[Highlight] = BaseColor::Blue.dark();
        }),
    });
    let history_appender = BoundedAppend::new(HISTORY_VIEW, max_lines, Into::into);
    siv.add_layer(
        LinearLayout::horizontal()
            .child(
                LinearLayout::vertical()
                    .child(Panel::new(
                        HistoryEditView::new(HISTORY_LEN)
                            .on_submit_mut(move |s, text| {
                                on_command(s, &mut controller, &history_appender, text)
                            })
                            .with_name(COMMAND_EDIT),
                    ))
                    .child(
                        Panel::new(
                            TextView::new("Type 'help' to learn about commands.\n")
                                .with_name(HISTORY_VIEW)
                                .scrollable()
                                .scroll_x(true)
                                .scroll_strategy(ScrollStrategy::StickToBottom),
                        )
                        .full_screen(),
                    )
                    .child(
                        Panel::new(
                            TextView::new("")
                                .with_name(LOG_VIEW)
                                .scrollable()
                                .scroll_x(true)
                                .scroll_strategy(ScrollStrategy::StickToBottom),
                        )
                        .full_screen(),
                    ),
            )
            .child(
                Panel::new(
                    TextView::new("")
                        .with_name(LINKS_VIEW)
                        .scrollable()
                        .scroll_x(true)
                        .scroll_strategy(ScrollStrategy::StickToBottom),
                )
                .full_screen(),
            ),
    );
}

const HELP: &[&str] = &[
    "For help on a specific command type 'help [command_name]'\n",
    "\n",
    "Valid commands are:\n",
    "\n",
    "clear        Clear this display.\n",
    "help         Display this list.\n",
    "quit         Close the application.\n",
    "\n",
    "clear-with   Clear the values current set with 'with-host', 'with-node' and 'with-lane'.\n",
    "command      Send a command to an existing link, or directly to a specified remote lane.\n",
    "link         Open a link to a remote lane.\n",
    "list         List all active links.\n",
    "map-command  Send a map command to an existing link, or directly to a specified remote lane.\n",
    "periodically Send a stream of commands to a specified target.\n",
    "query        Query the state of an active link.\n",
    "show-with    Show the values current set with 'with-host', 'with-node' and 'with-lane'.\n",
    "sync         Send a sync frame to an existing link.\n",
    "unlink       Send an unlink frame to an existing link.\n",
    "with-host    Execute subsequent commands against an implicit host.\n",
    "with-node    Execute subsequent commands against an implicit agent node URI.\n",
    "with-lane    Execute subsequent commands against an implicit lane.\n",
    "\n",
];

const CLEAR: &[&str] = &["Clear this display.\n", "\n", "clear\n", "\n"];

const HELP_HELP: &[&str] = &[
    "Provide help for a command.\n",
    "\n",
    "help command-name:(string)\n",
    "\n",
    "If not command name is specified, the list of valid command will be printed.\n",
    "\n",
];

const QUIT: &[&str] = &["Quit the application.\n", "\n", "quit\n", "\n"];

const CLEAR_WITH: &[&str] = &[
    "Clear this values set with the 'with-host', 'with-node' and 'with-lane' commands.\n",
    "\n",
    "clear-with\n",
    "\n",
];

const SHOW_WITH: &[&str] = &[
    "Print this values set with the 'with-host', 'with-node' and 'with-lane' commands.\n",
    "\n",
    "clear-with\n",
    "\n",
];

const COMMAND: &[&str] = &[
    "Send a command frame to a remote lane.\n",
    "In all cases, the body must be valid Recon.\n",
    "Recon values that contain white space must be quoted with backticks.\n",
    "Example: `@item \"name\"`\n",
    "\n",
    "command id:(integer) body:(recon)\n",
    "\n",
    "Send a command to the link with the given ID.\n",
    "\n",
    "command name:(string) body:(recon)\n",
    "\n",
    "Send a command to the link with the given name.\n",
    "\n",
    "command $target:(string) body:(recon)",
    "\n",
    "Send a command to an endpoint name defined with the 'target' command.\n",
    "\n",
    "command [--host|-h host_name] [--node|-n node_uri] [--lane|-l lane] body:(recon)\n",
    "\n",
    "Send a command to the specified lane. If a link is already open to that lane, it will be used.\n",
    "\n",
];

const MAP_COMMAND: &[&str] = &[
    "Send a map command frame to a remote lane.\n",
    "In all cases, the key and/or value must be valid Recon.\n",
    "Recon values that contain white space must be quoted with backticks.\n",
    "Example: `@item \"name\"`\n",
    "\n",
    "map-command id:(integer) body:(map_cmd)\n",
    "\n",
    "Send a command to the link with the given ID.\n",
    "\n",
    "map-command name:(string) body:(map_cmd)\n",
    "\n",
    "Send a command to the link with the given name.\n",
    "\n",
    "map-command $target:(string) body:(map_cmd)",
    "\n",
    "Send a command to an endpoint name defined with the 'target' command.\n",
    "\n",
    "map-command [--host|-h host_name] [--node|-n node_uri] [--lane|-l lane] body:(map_cmd)\n",
    "\n",
    "Send a command to the specified lane. If a link is already open to that lane, it will be used.\n",
    "\n",
    "Map commands are one of:\n",
    "\n",
    "1. update key:(recon) value:(recon)\n",
    "2. remove key:(recon)\n",
    "1. clear\n",
    "\n",
];

const LINK: &[&str] = &[
    "Open a link to a remote lane.\n",
    "\n",
    "link [--host|-h host_name:(string)] [--node|-n node_uri:(string)] [--lane|-l lane:(string)] ?[--name name:(string)] ?[--map|-m] ?[--sync]\n",
    "\n",
    "If a name is specified this can be used, instead of the link ID, to refer to the link.\n",
    "\n",
    "The 'map' flag will cause the link to interpret the events of the lane as map events.\n",
    "If the events do not have the correct format the link will fail.\n",
    "\n",
    "The 'sync' flag will cause the link to be synced as soon as it opens successfully.\n",
    "\n",
];

const PERIODICALLY: &[&str] = &[
    "Send a stream of commands to a specified target.\n",
    "\n",
    "periodically ?[--delay | -d delay:(duration)] [--kind | -k kind:(kind)] ?[--limit n:(usize)] $target:(string)\n",
    "\n",
    "Commands will be sent with an interval of 'delay'. Durations are specified as '1s' for 1 second, '2ms' for 2 milliseconds, etc.\n",
    "If not specified the default is 1 second.\n",
    "\n",
    "The 'kind' flag indicates the type of the messages that are sent. The supported kinds are:\n",
    "words    The messages are random words (as strings).\n",
    "n..m     Random integers in the range n to m (exclusive) (e.g. 1..10). These can be of type i32 or i64.\n",
    "\n",
    "The 'limit' flag will limit the number of commands to the specified number. If not supplied the stream is infinite.\n",
    "\n",
    "The target must be a named endpoint, defined with the 'target' command.\n",
    "\n",
];

const LIST: &[&str] = &["Show all open links.\n", "\n", "list\n", "\n"];

const SYNC: &[&str] = &[
    "Send a sync frame to an open link.\n",
    "\n",
    "sync id:(integer) | name:(string)\n",
    "\n",
];

const QUERY: &[&str] = &[
    "Query the state of an active link.\n",
    "\n",
    "query id:(integer) | name:(string)\n",
    "\n",
];

const TARGET: &[&str] = &[
    "Defined a named target for issuing commands.\n",
    "\n",
    "target [--host|-h host_name:(string)] [--node|-n node_uri:(string)] [--lane|-l lane:(string)] name:(string)\n",
    "\n",
];

const UNLINK: &[&str] = &[
    "Send an unlink frame to an open link.\n",
    "\n",
    "unlink id:(integer)\n",
    "\n",
    "Unlink the specified link.\n",
    "\n",
    "unlink --all\n",
    "\n",
    "Unlink all active links.\n",
    "\n",
];

const WITH_HOST: &[&str] = &[
    "Run subsequent command with an implicit remote host.\n",
    "\n",
    "with-host host_name:(string)\n",
    "\n",
];

const WITH_NODE: &[&str] = &[
    "Run subsequent command with an implicit node URI.\n",
    "\n",
    "with-node node_uri:(string)\n",
    "\n",
];

const WITH_LANE: &[&str] = &[
    "Run subsequent command with an implicit lane.\n",
    "\n",
    "with-lane lane:(string)\n",
    "\n",
];

const UNKNOWN: &[&str] = &["Unknown command."];

fn on_command(
    cursive: &mut Cursive,
    controller: &mut Controller,
    appender: &BoundedAppend<&'static str, Cow<'static, str>>,
    text: &str,
) {
    appender.append(cursive, Cow::Owned(format!("> {}\n", text)));

    let responses = match parse_app_command(text) {
        Ok(AppCommand::Help { command_name: None }) => {
            Some(HELP.iter().map(|s| Cow::Borrowed(*s)).collect())
        }
        Ok(AppCommand::Help {
            command_name: Some(cmd_name),
        }) => {
            let help_text = match cmd_name.as_str() {
                "clear" => CLEAR,
                "help" => HELP_HELP,
                "quit" => QUIT,
                "with-host" => WITH_HOST,
                "with-node" => WITH_NODE,
                "with-lane" => WITH_LANE,
                "show-with" => SHOW_WITH,
                "clear-with" => CLEAR_WITH,
                "link" => LINK,
                "map-command" => MAP_COMMAND,
                "periodically" => PERIODICALLY,
                "query" => QUERY,
                "sync" => SYNC,
                "target" => TARGET,
                "unlink" => UNLINK,
                "list" => LIST,
                "command" => COMMAND,
                _ => UNKNOWN,
            };
            Some(help_text.iter().map(|s| Cow::Borrowed(*s)).collect())
        }
        Ok(AppCommand::Quit) => {
            cursive.quit();
            Some(vec![])
        }
        Ok(AppCommand::Clear) => None,
        Ok(AppCommand::Controller(command)) => Some(
            controller
                .perform_action(*command)
                .into_iter()
                .map(|msg| format!("{}\n", msg))
                .map(Cow::Owned)
                .collect(),
        ),
        Err(msg) => Some(vec![
            Cow::Owned(format!("{}\n", msg)),
            Cow::Borrowed("Type 'help' to list valid commands.\n"),
        ]),
    };
    if let Some(resp) = responses {
        appender.append_many(cursive, resp);
    } else {
        appender.clear(cursive);
    }
}

fn format_display(display: DisplayResponse) -> StyledString {
    let line = format!("{}\n", display);
    let id = display.id;
    let colour = COLOURS[id % COLOURS.len()];
    StyledString::styled(line, colour)
}
