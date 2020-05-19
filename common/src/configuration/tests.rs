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

use tokio::fs::File;

use super::ConfigurationError;
use crate::model::{Attr, Item, Value};
use hamcrest2::assert_that;
use hamcrest2::prelude::*;
use std::path::PathBuf;

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
    assert_that!(&file, ok());
    let result = super::read_config_from(file.unwrap()).await;
    assert_that!(&result, ok());
    let items = result.unwrap();
    let complex = Value::Record(
        vec![Attr::with_value("name", 7)],
        vec![Item::of(1), Item::of(2), Item::of(3)],
    );
    assert_that!(
        items,
        eq(vec![
            Item::slot("first", 3),
            Item::slot("second", "hello"),
            Item::ValueItem(complex),
            Item::of(true)
        ])
    );
}

#[tokio::test]
async fn read_invalid_file() {
    let mut path = test_data_path();
    path.push("invalid.recon");

    let file = File::open(path).await;
    assert_that!(&file, ok());
    let result = super::read_config_from(file.unwrap()).await;
    assert!(matches!(result, Err(ConfigurationError::Parser(_))));
}
