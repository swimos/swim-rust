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

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC};

pub mod agency;
pub mod bounding_box;
pub mod counts;
pub mod route;
pub mod vehicle;

const XML_HEADER: &str = "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n";
const XML_INDENT_CHAR: char = ' ';
const XML_INDENT: usize = 4;

pub const URL_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');
