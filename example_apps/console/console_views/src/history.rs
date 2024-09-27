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

use std::{collections::VecDeque, sync::Arc};

use cursive::{
    direction::Direction,
    event::{AnyCb, Callback, Event, EventResult, Key},
    view::{Selector, ViewNotFound},
    views::EditView,
    Cursive, Printer, Rect, Vec2, View,
};

pub struct HistoryEditView {
    inner: EditView,
    history: History,
}

impl HistoryEditView {
    pub fn new(max_history: usize) -> Self {
        let history = History::new(max_history);

        HistoryEditView {
            inner: EditView::new(),
            history,
        }
    }

    #[must_use]
    pub fn on_submit<F>(mut self, callback: F) -> Self
    where
        F: Fn(&mut Cursive, &str) + Send + Sync + 'static,
    {
        self.set_on_submit(callback);
        self
    }

    pub fn set_on_submit<F>(&mut self, callback: F)
    where
        F: Fn(&mut Cursive, &str) + Send + Sync + 'static,
    {
        let HistoryEditView { inner, .. } = self;
        inner.set_on_submit(callback);
    }

    pub fn set_on_submit_mut<F>(&mut self, callback: F)
    where
        F: FnMut(&mut Cursive, &str) + Send + Sync + 'static,
    {
        let HistoryEditView { inner, .. } = self;
        inner.set_on_submit_mut(callback);
    }

    #[must_use]
    pub fn on_submit_mut<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut Cursive, &str) + Send + Sync + 'static,
    {
        self.set_on_submit_mut(callback);
        self
    }

    #[must_use]
    pub fn on_edit<F>(mut self, callback: F) -> Self
    where
        F: Fn(&mut Cursive, &str, usize) + Send + Sync + 'static,
    {
        self.set_on_edit(callback);
        self
    }

    pub fn set_on_edit<F>(&mut self, callback: F)
    where
        F: Fn(&mut Cursive, &str, usize) + Send + Sync + 'static,
    {
        let HistoryEditView { inner, .. } = self;
        inner.set_on_edit(callback);
    }

    pub fn set_on_edit_mut<F>(&mut self, callback: F)
    where
        F: FnMut(&mut Cursive, &str, usize) + Send + Sync + 'static,
    {
        let HistoryEditView { inner, .. } = self;
        inner.set_on_edit_mut(callback);
    }

    #[must_use]
    pub fn on_edit_mut<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&mut Cursive, &str, usize) + Send + Sync + 'static,
    {
        self.set_on_edit_mut(callback);
        self
    }

    pub fn get_content(&self) -> Arc<String> {
        self.inner.get_content()
    }

    pub fn set_content<S>(&mut self, content: S) -> Callback
    where
        S: Into<String>,
    {
        self.inner.set_content(content)
    }

    pub fn get_history(&self) -> &VecDeque<String> {
        &self.history.entries
    }
}

struct History {
    max: usize,
    entries: VecDeque<String>,
    index: Option<usize>,
}

impl History {
    fn new(max: usize) -> Self {
        History {
            max,
            entries: VecDeque::new(),
            index: None,
        }
    }

    fn push(&mut self, entry: String) {
        let History {
            max,
            entries,
            index,
        } = self;
        entries.push_back(entry);
        if entries.len() > *max {
            entries.pop_front();
        }
        *index = None;
    }

    fn get(&self) -> Option<&str> {
        let History { entries, index, .. } = self;
        (*index).and_then(|i| {
            let offset = entries.len() - (i + 1);
            entries.get(offset).map(String::as_str)
        })
    }

    fn incr(&mut self) -> bool {
        let History { entries, index, .. } = self;
        match *index {
            Some(i) => {
                if i + 1 == entries.len() {
                    false
                } else {
                    *index = Some(i + 1);
                    true
                }
            }
            None => {
                if entries.is_empty() {
                    false
                } else {
                    *index = Some(0);
                    true
                }
            }
        }
    }

    fn decr(&mut self) -> bool {
        let History { index, .. } = self;
        match *index {
            Some(i) => {
                if i == 0 {
                    *index = None;
                    true
                } else {
                    *index = Some(i - 1);
                    true
                }
            }
            None => false,
        }
    }
}

impl View for HistoryEditView {
    fn draw(&self, printer: &Printer) {
        self.inner.draw(printer)
    }

    fn layout(&mut self, dim: Vec2) {
        self.inner.layout(dim)
    }

    fn needs_relayout(&self) -> bool {
        self.inner.needs_relayout()
    }

    fn required_size(&mut self, constraint: Vec2) -> Vec2 {
        self.inner.required_size(constraint)
    }

    fn on_event(&mut self, event: Event) -> EventResult {
        match event {
            Event::Key(Key::Up) => {
                let HistoryEditView { inner, history } = self;
                if history.incr() {
                    if let Some(text) = history.get() {
                        inner.set_content(text);
                    }
                }
                EventResult::Consumed(None)
            }
            Event::Key(Key::Down) => {
                let HistoryEditView { inner, history } = self;
                if history.decr() {
                    inner.set_content(history.get().unwrap_or(""));
                }
                EventResult::Consumed(None)
            }
            Event::Key(Key::Enter) => {
                let HistoryEditView { inner, history } = self;
                history.push((*inner.get_content()).clone());
                let result = self.inner.on_event(Event::Key(Key::Enter));
                self.inner.set_content("");
                result.and(EventResult::Consumed(None))
            }
            ow => self.inner.on_event(ow),
        }
    }

    fn call_on_any(&mut self, s: &Selector, cb: AnyCb) {
        self.inner.call_on_any(s, cb)
    }

    fn focus_view(&mut self, s: &Selector) -> Result<EventResult, ViewNotFound> {
        self.inner.focus_view(s)
    }

    fn take_focus(&mut self, source: Direction) -> Result<EventResult, cursive::view::CannotFocus> {
        self.inner.take_focus(source)
    }

    fn important_area(&self, view_size: Vec2) -> Rect {
        self.inner.important_area(view_size)
    }
}
