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

use crate::utils::WatchStrategy;
use macro_helpers::str_to_ident;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use syn::{parse_macro_input, AttributeArgs, DeriveInput};

pub fn default_on_command() -> Ident {
    str_to_ident("on_command")
}

pub fn default_on_cue() -> Ident {
    str_to_ident("on_cue")
}

pub fn default_on_sync() -> Ident {
    str_to_ident("on_sync")
}

pub fn default_on_start() -> Ident {
    str_to_ident("on_start")
}

pub fn default_on_event() -> Ident {
    str_to_ident("on_event")
}

pub fn default_watch_strategy() -> WatchStrategy {
    WatchStrategy {
        ty: str_to_ident("Queue"),
        param: None,
    }
}

pub fn parse_strategy(s: String) -> WatchStrategy {
    let mut split = s.split('(');
    let ty = split.next().unwrap();

    let param = if let Some(rest) = split.next() {
        let param_str = rest.split(')').next().unwrap();
        Some(param_str.parse().unwrap())
    } else {
        None
    };

    WatchStrategy {
        ty: str_to_ident(ty),
        param,
    }
}

pub fn derive<F>(args: TokenStream, input: TokenStream, f: F) -> TokenStream
where
    F: Fn(AttributeArgs, DeriveInput) -> TokenStream,
{
    let input = parse_macro_input!(input as DeriveInput);
    let args = parse_macro_input!(args as AttributeArgs);

    f(args, input)
}
