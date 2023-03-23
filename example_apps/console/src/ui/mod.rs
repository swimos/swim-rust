// Copyright 2015-2023 Swim Inc.
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

use crate::model::{parse_app_command, DisplayResponse};
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
    log_appender: BoundedAppend<&'static str, String>,
}

impl CursiveUIUpdater {
    pub fn new(sink: cursive::CbSink, timeout: Duration, max_lines: usize) -> Self {
        CursiveUIUpdater {
            sink,
            timeout,
            link_appender: BoundedAppend::new(LINKS_VIEW, max_lines, format_display),
            log_appender: BoundedAppend::new(LOG_VIEW, max_lines, format_log_msg),
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
            UIUpdate::LogMessage(msg) => {
                let log_appender = *log_appender;
                sink.send_timeout(
                    Box::new(move |s| {
                        log_appender.append(s, msg);
                    }),
                    *timeout,
                )?;
            }
        }
        Ok(())
    }
}

const HISTORY_VIEW: &str = "history";
const LINKS_VIEW: &str = "links";
const LOG_VIEW: &str = "log";

const COMMAND_EDIT: &str = "command";

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
                        HistoryEditView::new(5)
                            .on_submit_mut(move |s, text| {
                                on_command(s, &mut controller, &history_appender, text)
                            })
                            .with_name(COMMAND_EDIT),
                    ))
                    .child(
                        Panel::new(
                            TextView::new("")
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
    "command      Send a command to an existing link or directly to a specified remote lane.\n",
    "link         Open a link to a remote lane.\n",
    "list         List all active links.\n",
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
    "\n",
    "command [id:(integer)] body:(recon)\n",
    "\n",
    "Send a command to the link with the given ID.\n",
    "\n",
    "command [name:(string)] body:(recon)\n",
    "\n",
    "Send a command to the link with the given name.\n",
    "\n",
    "command [--host|-h host_name] [--node|-n node_uri] [--lane|-l lane] body:(recon)\n",
    "\n",
    "Send a command to the specified lane. If a link is already open to that lane, it will be used.\n",
    "\n",
];

const LINK: &[&str] = &[
    "Open a link to a remote lane.\n",
    "\n",
    "link [--host|-h host_name:(string)] [--node|-n node_uri:(string)] [--lane|-l lane:(string)] [name:(string)]\n",
    "\n",
];

const LIST: &[&str] = &["Show all open links.\n", "\n", "list\n", "\n"];

const SYNC: &[&str] = &[
    "Send a sync frame to an open link.\n",
    "\n",
    "sync id:(integer)\n",
    "\n",
];

const UNLINK: &[&str] = &[
    "Send an unlink frame to an open link.\n",
    "\n",
    "unlink id:(integer)\n",
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
    let command_parts = text.split_whitespace().collect::<Vec<_>>();

    let responses = match command_parts.as_slice() {
        ["help"] => Some(HELP.iter().map(|s| Cow::Borrowed(*s)).collect()),
        ["help", cmd_name] => {
            let help_text = match *cmd_name {
                "clear" => CLEAR,
                "help" => HELP_HELP,
                "quit" => QUIT,
                "with-host" => WITH_HOST,
                "with-node" => WITH_NODE,
                "with-lane" => WITH_LANE,
                "show-with" => SHOW_WITH,
                "clear-with" => CLEAR_WITH,
                "link" => LINK,
                "sync" => SYNC,
                "unlink" => UNLINK,
                "list" => LIST,
                "command" => COMMAND,
                _ => UNKNOWN,
            };
            Some(help_text.iter().map(|s| Cow::Borrowed(*s)).collect())
        }
        ["quit"] => {
            cursive.quit();
            Some(vec![])
        }
        ["clear"] => None,
        _ => {
            let msgs = match parse_app_command(command_parts.as_slice()) {
                Ok(command) => controller
                    .perform_action(command)
                    .into_iter()
                    .map(|msg| format!("{}\n", msg))
                    .map(Cow::Owned)
                    .collect(),
                Err(msg) => {
                    vec![
                        Cow::Owned(format!("{}\n", msg)),
                        Cow::Borrowed("Type 'help' to list valid commands.\n"),
                    ]
                }
            };
            Some(msgs)
        }
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

fn format_log_msg(msg: String) -> StyledString {
    let line = format!("{}\n", msg);
    StyledString::styled(line, BaseColor::Red.light())
}
