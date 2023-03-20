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

use cursive::theme::Color::TerminalDefault;
use cursive::theme::PaletteColor::*;
use cursive::{
    theme::{BaseColor, BorderStyle, Palette, Theme},
    view::{Nameable, Resizable, ScrollStrategy, Scrollable},
    views::{EditView, LinearLayout, Panel, TextView},
    Cursive, CursiveExt, With,
};
use swim::agent::lanes::command;
use ui::history::HistoryEditView;
mod controller;
mod model;
mod oneshot;
mod runtime;
mod shared_state;
mod ui;

fn main() {
    let mut siv = Cursive::default();

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
    siv.add_layer(
        LinearLayout::horizontal()
            .child(
                LinearLayout::vertical()
                    .child(Panel::new(
                        HistoryEditView::new(5)
                            .on_submit(on_command)
                            .with_name("command"),
                    ))
                    .child(
                        Panel::new(
                            TextView::new("")
                                .with_name("history")
                                .scrollable()
                                .scroll_strategy(ScrollStrategy::StickToBottom),
                        )
                        .full_screen(),
                    ),
            )
            .child(Panel::new(TextView::new("World")).full_screen()),
    );

    siv.run();
}

const HELP: &[&str] = &[
    "Valid commands are:\n",
    "help\t\tDisplay this list.\n",
    "quit\t\tClose the application.\n",
    "clear\t\tClear this display.\n"
];

fn on_command(cursive: &mut Cursive, text: &str) {
    if text == "quit" {
        cursive.quit();
    }
    cursive.call_on_name("history", |view: &mut TextView| {
        view.append(format!("> {}\n", text))
    
    });
    let command_parts = text.split_whitespace().collect::<Vec<_>>();
    
    let responses = match command_parts.as_slice() {
        ["help"] => {
            Some(HELP.iter().map(|s| Cow::Borrowed(*s)).collect())
        },
        ["quit"] => {
            cursive.quit();
            Some(vec![])
        }
        ["clear"] => None,
        _ => {
            Some(vec![Cow::Borrowed("Incorrect command. Type 'help' to list valid commands.")])
        }
    };
    cursive.call_on_name("history", move |view: &mut TextView| {
        if let Some(resp) = responses {
            for response in resp {
                view.append(response);
            }
        } else {
            view.set_content("");
        }
    });
}
