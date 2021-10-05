// Copyright 2015-2021 SWIM.AI inc.
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

use tokio::fs::File;

use swim_model::{Attr, Item, Value};
use std::path::PathBuf;
use crate::parser::async_parser::AsyncParseError;

fn test_data_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("test-data");
    path
}

#[tokio::test]
async fn read_configuration_from_file() {
    let mut path = test_data_path();
    path.push("test.recon");

    let file = File::open(path).await;
    assert!(file.is_ok());
    let result = super::parse_recon_document(file.unwrap()).await;
    assert!(result.is_ok());
    let items = result.unwrap();
    let complex = Value::Record(
        vec![Attr::with_value("name", 7u32)],
        vec![Item::of(1u32), Item::of(2u32), Item::of(3u32)],
    );
    assert_eq!(
        items,
        vec![
            Item::slot("first", 3u32),
            Item::slot("second", "hello"),
            Item::ValueItem(complex),
            Item::of(true)
        ]
    );
}

#[tokio::test]
async fn read_invalid_file() {
    let mut path = test_data_path();
    path.push("invalid.recon");

    let file = File::open(path).await;
    assert!(file.is_ok());
    let result = super::parse_recon_document(file.unwrap()).await;
    assert!(matches!(result, Err(AsyncParseError::Parser(_))));
}
