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

//! Utilities for working with the Recon markup format.
//!
//! This create provides a number of utilities for working with the Recon markup format.
//! These consist of:
//!
//! - Recon parser.
//! - Recon printer that will format types that support the [`swimos_form::Form`] trait to strings.
//! - Comparator for Recon strings that does not require them to be deserialized.
//! - Hash function for Recon strings (that will produce the same hash for strings that represent equal values).

mod buffers;
mod comparator;
mod hasher;
mod printer;
mod recon_parser;

pub use buffers::write_recon;
pub use comparator::compare_recon_values;
pub use hasher::{recon_hash, HashError};
pub use printer::{print_recon, print_recon_compact, print_recon_pretty};

/// Recon format parsers.
///
/// - Parser that expects a single recon value, with or without comments.
/// - Parser for Recon configuration files where the file contains the contents of a Recon record, with or without comments.
/// - Extractors to parse only the first attribute of a Recon record, leaving the remainder as an uninterpreted string.
pub mod parser {
    pub use crate::recon_parser::{
        extract_header, extract_header_str, parse_recognize, parse_text_token, HeaderPeeler,
        MessageExtractError, ParseError, Span,
    };
    #[cfg(feature = "async_parser")]
    pub use crate::recon_parser::{parse_recon_document, AsyncParseError, RecognizerDecoder};
}
