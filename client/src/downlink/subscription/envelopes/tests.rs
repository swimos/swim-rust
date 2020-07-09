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

use hamcrest2::assert_that;
use hamcrest2::prelude::*;
use swim_form_old::Form;

use super::*;
use crate::downlink::Message;
use common::model::Value::Int32Value;
use common::warp::envelope::LinkMessage;

fn path() -> AbsolutePath {
    AbsolutePath::new(url::Url::parse("ws://127.0.0.1/").unwrap(), "node", "lane")
}

#[test]
fn unlink_value_command_to_envelope() {
    let expected = LinkMessage::unlink("node", "lane");
    let (host, envelope) = value_envelope(&path(), Command::Unlink);
    assert_eq!(host, url::Url::parse("ws://127.0.0.1/").unwrap());
    assert_that!(envelope, eq(expected));
}

#[test]
fn unlinked_value_message_from_envelope() {
    let env = LinkMessage::unlinked("node", "lane");
    let result = value::from_envelope(env);
    assert_that!(result, eq(Message::Unlinked));
}

#[test]
fn sync_value_command_to_envelope() {
    let expected = LinkMessage::sync("node", "lane");
    let (host, envelope) = value_envelope(&path(), Command::Sync);
    assert_eq!(host, url::Url::parse("ws://127.0.0.1/").unwrap());
    assert_that!(envelope, eq(expected));
}

#[test]
fn linked_value_message_from_envelope() {
    let env = LinkMessage::linked("node", "lane");
    let result = value::from_envelope(env);
    assert_that!(result, eq(Message::Linked));
}

#[test]
fn synced_value_message_from_envelope() {
    let env = LinkMessage::synced("node", "lane");
    let result = value::from_envelope(env);
    assert_that!(result, eq(Message::Synced));
}

#[test]
fn data_value_command_to_envelope() {
    let expected = LinkMessage::make_command("node", "lane", Some(Int32Value(5)));
    let (host, envelope) = value_envelope(
        &path(),
        Command::Action(SharedValue::new(Value::Int32Value(5))),
    );
    assert_eq!(host, url::Url::parse("ws://127.0.0.1/").unwrap());
    assert_that!(envelope, eq(expected));
}

#[test]
fn data_value_message_from_envelope() {
    let env = LinkMessage::make_event("node", "lane", Some(Int32Value(7)));
    let result = value::from_envelope(env);
    assert_that!(result, eq(Message::Action(Int32Value(7))))
}

#[test]
fn unlink_map_command_to_envelope() {
    let expected = LinkMessage::unlink("node", "lane");
    let (host, envelope) = map_envelope(&path(), Command::Unlink);
    assert_eq!(host, url::Url::parse("ws://127.0.0.1/").unwrap());
    assert_that!(envelope, eq(expected));
}

#[test]
fn sync_map_command_to_envelope() {
    let expected = LinkMessage::sync("node", "lane");
    let (host, envelope) = map_envelope(&path(), Command::Sync);
    assert_eq!(host, url::Url::parse("ws://127.0.0.1/").unwrap());
    assert_that!(envelope, eq(expected));
}

#[test]
fn clear_map_command_to_envelope() {
    let rep = Form::into_value(UntypedMapModification::<Value>::Clear);

    let expected = LinkMessage::make_command("node", "lane", Some(rep));
    let (host, envelope) = map_envelope(&path(), Command::Action(UntypedMapModification::Clear));
    assert_eq!(host, url::Url::parse("ws://127.0.0.1/").unwrap());
    assert_that!(envelope, eq(expected));
}

#[test]
fn take_map_command_to_envelope() {
    let rep = Form::into_value(UntypedMapModification::<Value>::Take(7));

    let expected = LinkMessage::make_command("node", "lane", Some(rep));
    let (host, envelope) = map_envelope(&path(), Command::Action(UntypedMapModification::Take(7)));
    assert_eq!(host, url::Url::parse("ws://127.0.0.1/").unwrap());
    assert_that!(envelope, eq(expected));
}

#[test]
fn skip_map_command_to_envelope() {
    let rep = Form::into_value(UntypedMapModification::<Value>::Skip(7));

    let expected = LinkMessage::make_command("node", "lane", Some(rep));
    let (host, envelope) = map_envelope(&path(), Command::Action(UntypedMapModification::Skip(7)));
    assert_eq!(host, url::Url::parse("ws://127.0.0.1/").unwrap());
    assert_that!(envelope, eq(expected));
}

#[test]
fn remove_map_command_to_envelope() {
    let action = UntypedMapModification::<Value>::Remove(Value::text("key"));

    let rep = Form::as_value(&action);

    let expected = LinkMessage::make_command("node", "lane", Some(rep));
    let (host, envelope) = map_envelope(
        &path(),
        Command::Action(UntypedMapModification::Remove(Value::text("key"))),
    );
    assert_eq!(host, url::Url::parse("ws://127.0.0.1/").unwrap());
    assert_that!(envelope, eq(expected));
}

#[test]
fn insert_map_command_to_envelope() {
    let action = UntypedMapModification::Insert(Value::text("key"), Value::text("value"));

    let rep = Form::as_value(&action);

    let expected = LinkMessage::make_command("node", "lane", Some(rep));

    let arc_action =
        UntypedMapModification::Insert(Value::text("key"), Arc::new(Value::text("value")));

    let (host, envelope) = map_envelope(&path(), Command::Action(arc_action));
    assert_eq!(host, url::Url::parse("ws://127.0.0.1/").unwrap());
    assert_that!(envelope, eq(expected));
}

#[test]
fn unlinked_map_message_from_envelope() {
    let env = LinkMessage::unlinked("node", "lane");
    let result = map::from_envelope(env);
    assert_that!(result, eq(Message::Unlinked));
}

#[test]
fn linked_map_message_from_envelope() {
    let env = LinkMessage::linked("node", "lane");
    let result = map::from_envelope(env);
    assert_that!(result, eq(Message::Linked));
}

#[test]
fn synced_map_message_from_envelope() {
    let env = LinkMessage::synced("node", "lane");
    let result = map::from_envelope(env);
    assert_that!(result, eq(Message::Synced));
}

#[test]
fn clear_map_message_from_envelope() {
    let rep = Form::into_value(UntypedMapModification::<Value>::Clear);
    let env = LinkMessage::make_event("node", "lane", Some(rep));
    let result = map::from_envelope(env);
    assert_that!(result, eq(Message::Action(UntypedMapModification::Clear)))
}

#[test]
fn take_map_message_from_envelope() {
    let rep = Form::into_value(UntypedMapModification::<Value>::Take(14));
    let env = LinkMessage::make_event("node", "lane", Some(rep));
    let result = map::from_envelope(env);
    assert_that!(
        result,
        eq(Message::Action(UntypedMapModification::Take(14)))
    )
}

#[test]
fn skip_map_message_from_envelope() {
    let rep = Form::into_value(UntypedMapModification::<Value>::Skip(1));
    let env = LinkMessage::make_event("node", "lane", Some(rep));
    let result = map::from_envelope(env);
    assert_that!(result, eq(Message::Action(UntypedMapModification::Skip(1))))
}

#[test]
fn remove_map_message_from_envelope() {
    let action = UntypedMapModification::Remove(Value::text("key"));

    let rep = Form::as_value(&action);
    let env = LinkMessage::make_event("node", "lane", Some(rep));
    let result = map::from_envelope(env);
    assert_that!(result, eq(Message::Action(action)))
}

#[test]
fn insert_map_message_from_envelope() {
    let action = UntypedMapModification::Insert(Value::text("key"), Value::text("value"));

    let rep = Form::as_value(&action);
    let env = LinkMessage::make_event("node", "lane", Some(rep));
    let result = map::from_envelope(env);
    assert_that!(result, eq(Message::Action(action)))
}
