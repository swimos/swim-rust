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

use console::style;

pub fn print_success(msg: impl AsRef<str>) {
    println!("{}", style(msg.as_ref()).green().bold());
}

pub fn print_error(msg: impl AsRef<str>) {
    println!("{}", style(msg.as_ref()).red().bold());
}
