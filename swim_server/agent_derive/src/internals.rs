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

use crate::utils::{Callback, CallbackKind};
use macro_helpers::str_to_ident;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use syn::{parse_macro_input, AttributeArgs, DeriveInput};

const DEFAULT_ON_START: &str = "on_start";
const DEFAULT_ON_COMMAND: &str = "on_command";
const DEFAULT_ON_EVENT: &str = "on_event";
const DEFAULT_ON_CUE: &str = "on_cue";
const DEFAULT_ON_SYNC: &str = "on_sync";

pub fn default_on_command() -> Ident {
    str_to_ident(DEFAULT_ON_COMMAND)
}

pub fn default_on_cue() -> Ident {
    str_to_ident(DEFAULT_ON_CUE)
}

pub fn default_on_sync() -> Ident {
    str_to_ident(DEFAULT_ON_SYNC)
}

pub fn default_on_start() -> Ident {
    str_to_ident(DEFAULT_ON_START)
}

pub fn default_on_event() -> Ident {
    str_to_ident(DEFAULT_ON_EVENT)
}

pub fn parse_callback(
    callback: &Option<darling::Result<String>>,
    task_name: Ident,
    kind: CallbackKind,
) -> Option<Callback> {
    if let Some(name) = callback {
        if let Ok(name) = name {
            Some(Callback {
                task_name,
                func_name: str_to_ident(&name),
                kind,
            })
        } else {
            match kind {
                CallbackKind::Start => Some(Callback {
                    task_name,
                    func_name: default_on_start(),
                    kind,
                }),
                CallbackKind::Command => Some(Callback {
                    task_name,
                    func_name: default_on_command(),
                    kind,
                }),
                CallbackKind::Event => Some(Callback {
                    task_name,
                    func_name: default_on_event(),
                    kind,
                }),
                CallbackKind::Cue => Some(Callback {
                    task_name,
                    func_name: default_on_cue(),
                    kind,
                }),
                CallbackKind::Sync => Some(Callback {
                    task_name,
                    func_name: default_on_sync(),
                    kind,
                }),
            }
        }
    } else {
        None
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
