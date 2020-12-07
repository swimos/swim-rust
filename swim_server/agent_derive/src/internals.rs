// Copyright 2015-2020 SWIM.AI inc.
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

use proc_macro2::{Ident, Span};

pub fn default_on_command() -> Ident {
    to_ident("on_command".to_string())
}

pub fn default_on_start() -> Ident {
    to_ident("on_start".to_string())
}

pub fn default_on_event() -> Ident {
    to_ident("on_event".to_string())
}

pub fn to_ident(value: String) -> Ident {
    Ident::new(&value, Span::call_site())
}
