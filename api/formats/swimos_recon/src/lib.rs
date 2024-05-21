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

mod comparator;
mod hasher;
mod printer;
mod recon_parser;

pub use comparator::compare_recon_values;
pub use hasher::{recon_hash, HashError};
pub use printer::{print_recon, print_recon_compact, print_recon_pretty};

pub mod parser {
    pub use crate::recon_parser::{
        extract_header, extract_header_str, parse_recognize, parse_recon_document,
        parse_text_token, AsyncParseError, HeaderPeeler, MessageExtractError, ParseError,
        RecognizerDecoder, Span,
    };
}
