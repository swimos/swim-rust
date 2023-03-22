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

use cursive::{Cursive, utils::markup::StyledString, views::TextView};

pub struct BoundedAppend<S, L> {
    label: S,
    max_lines: usize,
    format: fn(L) -> StyledString,
}

impl<S: Clone, L> Clone for BoundedAppend<S, L> {
    fn clone(&self) -> Self {
        Self { label: self.label.clone(), max_lines: self.max_lines.clone(), format: self.format.clone() }
    }
}

impl<S: Copy, L> Copy for BoundedAppend<S, L> {}

impl<S: AsRef<str>, L> BoundedAppend<S, L> {

    pub fn new(label: S, max_lines: usize, format: fn(L) -> StyledString) -> Self {
            BoundedAppend { label, max_lines, format }
        }

    pub fn append(&self, s: &mut Cursive, line: L) {
        self.append_many(s, std::iter::once(line))
    }

    pub fn append_many(&self, s: &mut Cursive, it: impl IntoIterator<Item = L>) {
        let BoundedAppend { label, max_lines, format } = self;
        let styled = it.into_iter().map(format);
        let max = *max_lines;
        s.call_on_name(label.as_ref(), move |view: &mut TextView| {
            let content = view.get_shared_content();
            for s in styled {
                content.append(s);
            }
            content.with_content(move |content| {
                let diff = content.spans_raw().len().saturating_sub(max);
                if diff > 0 {
                    content.remove_spans(0..diff);
                    content.compact();
                }
            })
        });
    }

    pub fn clear(&self, s: &mut Cursive) {
        let BoundedAppend { label, .. } = self;
        s.call_on_name(label.as_ref(), |view: &mut TextView| view.set_content(""));
    }

}