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

use form::Form;
use hamcrest2::assert_that;
use hamcrest2::prelude::*;

use super::*;
use crate::downlink::Message;
use common::model::Value::Int32Value;

fn path() -> AbsolutePath {
    AbsolutePath::new("host", "node", "lane")
}

#[test]
fn unlink_value_command_to_envelope() {
    let expected = Envelope::unlink("node".to_string(), "lane".to_string());
    let (host, envelope) = value_envelope(&path(), Command::Unlink);
    assert_that!(host, eq("host"));
    assert_that!(envelope, eq(expected));
}

#[test]
fn unlinked_value_message_from_envelope() {
    let env = Envelope::unlinked("node".to_string(), "lane".to_string());
    let result = value::try_from_envelope(env);
    assert_that!(result, eq(Ok(Message::Unlinked)));
}

#[test]
fn sync_value_command_to_envelope() {
    let expected = Envelope::sync("node".to_string(), "lane".to_string());
    let (host, envelope) = value_envelope(&path(), Command::Sync);
    assert_that!(host, eq("host"));
    assert_that!(envelope, eq(expected));
}

#[test]
fn linked_value_message_from_envelope() {
    let env = Envelope::linked("node".to_string(), "lane".to_string());
    let result = value::try_from_envelope(env);
    assert_that!(result, eq(Ok(Message::Linked)));
}

#[test]
fn synced_value_message_from_envelope() {
    let env = Envelope::synced("node".to_string(), "lane".to_string());
    let result = value::try_from_envelope(env);
    assert_that!(result, eq(Ok(Message::Synced)));
}

#[test]
fn data_value_command_to_envelope() {
    let expected = Envelope::command("node".to_string(), "lane".to_string(), Some(Int32Value(5)));
    let (host, envelope) = value_envelope(
        &path(),
        Command::Action(SharedValue::new(Value::Int32Value(5))),
    );
    assert_that!(host, eq("host"));
    assert_that!(envelope, eq(expected));
}

#[test]
fn data_value_message_from_envelope() {
    let env = Envelope::event("node".to_string(), "lane".to_string(), Some(Int32Value(7)));
    let result = value::try_from_envelope(env);
    assert_that!(result, eq(Ok(Message::Action(Int32Value(7)))))
}

#[test]
fn unlink_map_command_to_envelope() {
    let expected = Envelope::unlink("node".to_string(), "lane".to_string());
    let (host, envelope) = map_envelope(&path(), Command::Unlink);
    assert_that!(host, eq("host"));
    assert_that!(envelope, eq(expected));
}

#[test]
fn sync_map_command_to_envelope() {
    let expected = Envelope::sync("node".to_string(), "lane".to_string());
    let (host, envelope) = map_envelope(&path(), Command::Sync);
    assert_that!(host, eq("host"));
    assert_that!(envelope, eq(expected));
}

#[test]
fn clear_map_command_to_envelope() {
    let rep = Form::into_value(MapModification::Clear);

    let expected = Envelope::command("node".to_string(), "lane".to_string(), Some(rep));
    let (host, envelope) = map_envelope(&path(), Command::Action(MapModification::Clear));
    assert_that!(host, eq("host"));
    assert_that!(envelope, eq(expected));
}

#[test]
fn take_map_command_to_envelope() {
    let rep = Form::into_value(MapModification::Take(7));

    let expected = Envelope::command("node".to_string(), "lane".to_string(), Some(rep));
    let (host, envelope) = map_envelope(&path(), Command::Action(MapModification::Take(7)));
    assert_that!(host, eq("host"));
    assert_that!(envelope, eq(expected));
}

#[test]
fn skip_map_command_to_envelope() {
    let rep = Form::into_value(MapModification::Skip(7));

    let expected = Envelope::command("node".to_string(), "lane".to_string(), Some(rep));
    let (host, envelope) = map_envelope(&path(), Command::Action(MapModification::Skip(7)));
    assert_that!(host, eq("host"));
    assert_that!(envelope, eq(expected));
}

#[test]
fn remove_map_command_to_envelope() {
    let action = MapModification::Remove(Value::text("key"));

    let rep = Form::as_value(&action);

    let expected = Envelope::command("node".to_string(), "lane".to_string(), Some(rep));
    let (host, envelope) = map_envelope(
        &path(),
        Command::Action(MapModification::Remove(Value::text("key"))),
    );
    assert_that!(host, eq("host"));
    assert_that!(envelope, eq(expected));
}

#[test]
fn insert_map_command_to_envelope() {
    let action = MapModification::Insert(Value::text("key"), Value::text("value"));

    let rep = Form::as_value(&action);

    let expected = Envelope::command("node".to_string(), "lane".to_string(), Some(rep));

    let arc_action = MapModification::Insert(Value::text("key"), Arc::new(Value::text("value")));

    let (host, envelope) = map_envelope(&path(), Command::Action(arc_action));
    assert_that!(host, eq("host"));
    assert_that!(envelope, eq(expected));
}

#[test]
fn unlinked_map_message_from_envelope() {
    let env = Envelope::unlinked("node".to_string(), "lane".to_string());
    let result = map::try_from_envelope(env);
    assert_that!(result, eq(Ok(Message::Unlinked)));
}

#[test]
fn linked_map_message_from_envelope() {
    let env = Envelope::linked("node".to_string(), "lane".to_string());
    let result = map::try_from_envelope(env);
    assert_that!(result, eq(Ok(Message::Linked)));
}

#[test]
fn synced_map_message_from_envelope() {
    let env = Envelope::synced("node".to_string(), "lane".to_string());
    let result = map::try_from_envelope(env);
    assert_that!(result, eq(Ok(Message::Synced)));
}

#[test]
fn clear_map_message_from_envelope() {
    let rep = Form::into_value(MapModification::Clear);
    let env = Envelope::event("node".to_string(), "lane".to_string(), Some(rep));
    let result = map::try_from_envelope(env);
    assert_that!(result, eq(Ok(Message::Action(MapModification::Clear))))
}

#[test]
fn take_map_message_from_envelope() {
    let rep = Form::into_value(MapModification::Take(14));
    let env = Envelope::event("node".to_string(), "lane".to_string(), Some(rep));
    let result = map::try_from_envelope(env);
    assert_that!(result, eq(Ok(Message::Action(MapModification::Take(14)))))
}

#[test]
fn skip_map_message_from_envelope() {
    let rep = Form::into_value(MapModification::Skip(1));
    let env = Envelope::event("node".to_string(), "lane".to_string(), Some(rep));
    let result = map::try_from_envelope(env);
    assert_that!(result, eq(Ok(Message::Action(MapModification::Skip(1)))))
}

#[test]
fn remove_map_message_from_envelope() {
    let action = MapModification::Remove(Value::text("key"));

    let rep = Form::as_value(&action);
    let env = Envelope::event("node".to_string(), "lane".to_string(), Some(rep));
    let result = map::try_from_envelope(env);
    assert_that!(result, eq(Ok(Message::Action(action))))
}

#[test]
fn insert_map_message_from_envelope() {
    let action = MapModification::Insert(Value::text("key"), Value::text("value"));

    let rep = Form::as_value(&action);
    let env = Envelope::event("node".to_string(), "lane".to_string(), Some(rep));
    let result = map::try_from_envelope(env);
    assert_that!(result, eq(Ok(Message::Action(action))))
}
