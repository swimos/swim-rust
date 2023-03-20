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

use cursive::{views::{TextView, LinearLayout, Panel, EditView}, Cursive, view::{ScrollStrategy, Resizable, Nameable, Scrollable}, theme::{Theme, BorderStyle, Palette, BaseColor}, With, CursiveExt};
use cursive::theme::Color::TerminalDefault;
use cursive::theme::PaletteColor::*;
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
                    .child(
                        Panel::new(
                            HistoryEditView::new(4)
                                .on_submit(on_command)
                                .with_name("command")
                                
                        )
                    )
                    .child(
                        Panel::new(
                            TextView::new("")
                                .with_name("history")
                                .scrollable()
                                .scroll_strategy(ScrollStrategy::StickToBottom)
                        )
                        .full_screen()
                    )
            )
            .child(
                Panel::new(TextView::new("World"))
                    .full_screen()
            )
    );

	siv.run();
}

fn on_command(cursive: &mut Cursive, text: &str) {
    if text == "quit" {
        cursive.quit();
    }
    cursive.call_on_name("history", |view: &mut TextView| {
        view.append(format!("> {}\n", text))
    });
    
    if let Some(cb) = cursive.call_on_name("command", |view: &mut EditView| {
        view.set_content("")
    }) {
        cb(cursive);
    }
}