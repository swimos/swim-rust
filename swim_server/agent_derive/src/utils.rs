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

use core::fmt;
use macro_helpers::Context;
use proc_macro2::{Ident, Span};
use std::fmt::{Display, Formatter};
use syn::{Data, DeriveInput};

const SWIM_AGENT: &str = "Swim agent";
const LIFECYCLE: &str = "Lifecycle";

pub fn get_task_struct_name(name: &str) -> Ident {
    Ident::new(&format!("{}Task", name), Span::call_site())
}

#[derive(Debug)]
pub enum InputAstType {
    Agent,
    Lifecycle,
}

impl Display for InputAstType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            InputAstType::Agent => write!(f, "{}", SWIM_AGENT),
            InputAstType::Lifecycle => write!(f, "{}", LIFECYCLE),
        }
    }
}

pub fn validate_input_ast(
    input_ast: &DeriveInput,
    ty: InputAstType,
    context: &mut Context,
) -> Result<(), ()> {
    match &input_ast.data {
        Data::Enum(_) => {
            context.error_spanned_by(input_ast, format!("{} cannot be created from Enum.", ty));
            Err(())
        }
        Data::Union(_) => {
            context.error_spanned_by(input_ast, format!("{} cannot be created from Union.", ty));
            Err(())
        }
        _ => {
            if !input_ast.generics.params.is_empty() {
                context
                    .error_spanned_by(input_ast, format!("{} cannot have generic parameters.", ty));
                Err(())
            } else {
                Ok(())
            }
        }
    }
}
