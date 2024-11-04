// Copyright 2015-2024 Swim Inc.
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

#[cfg(feature = "json")]
pub fn create_kafka_props() -> std::collections::HashMap<String, String> {
    [
        ("bootstrap.servers", "datagen.nstream.cloud:9092"),
        ("message.timeout.ms", "5000"),
        ("group.id", "rust-consumer-test"),
        ("auto.offset.reset", "smallest"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}
