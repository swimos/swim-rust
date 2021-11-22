// Copyright 2015-2021 Swim Inc.
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

use super::*;
use crate::agent::lane::tests::ExactlyOnce;
use stm::transaction::atomically;

#[test]
fn default_summary() {
    let default: TransactionSummary<i32, String> = Default::default();
    assert_eq!(default.coordination_id, 0);
    assert!(!default.clear);
    assert!(default.changes.is_empty());
}

#[test]
fn summary_with_id() {
    let with_id: TransactionSummary<i32, String> = TransactionSummary::with_id(67);
    assert_eq!(with_id.coordination_id, 67);
    assert!(!with_id.clear);
    assert!(with_id.changes.is_empty());
}

#[test]
fn create_clear_summary() {
    let summary: TransactionSummary<Value, String> = TransactionSummary::clear();
    assert_eq!(summary.coordination_id, 0);
    assert!(summary.clear);
    assert!(summary.changes.is_empty());
}

#[test]
fn make_update() {
    let key = Value::Int32Value(2);
    let value = Arc::new(4);
    let summary = TransactionSummary::make_update(key.clone(), value.clone());
    assert_eq!(summary.coordination_id, 0);
    assert!(!summary.clear);
    assert_eq!(summary.changes.len(), 1);
    assert!(
        matches!(summary.changes.get(&key), Some(EntryModification::Update(v)) if Arc::ptr_eq(v, &value))
    );
}

#[test]
fn make_removal() {
    let key = Value::Int32Value(2);
    let summary: TransactionSummary<Value, i32> = TransactionSummary::make_removal(key.clone());
    assert_eq!(summary.coordination_id, 0);
    assert!(!summary.clear);
    assert_eq!(summary.changes.len(), 1);
    assert!(matches!(
        summary.changes.get(&key),
        Some(EntryModification::Remove)
    ));
}

#[test]
fn update_existing_no_clear() {
    let key = Value::Int32Value(2);
    let value = Arc::new(4);
    let summary = TransactionSummary::default();
    let updated = summary.update(key.clone(), value.clone());
    assert_eq!(updated.coordination_id, 0);
    assert!(!updated.clear);
    assert_eq!(updated.changes.len(), 1);
    assert!(
        matches!(updated.changes.get(&key), Some(EntryModification::Update(v)) if Arc::ptr_eq(v, &value))
    );
}

#[test]
fn update_existing_clear() {
    let key = Value::Int32Value(2);
    let value = Arc::new(4);
    let summary = TransactionSummary::clear();
    let updated = summary.update(key.clone(), value.clone());
    assert_eq!(updated.coordination_id, 0);
    assert!(updated.clear);
    assert_eq!(updated.changes.len(), 1);
    assert!(
        matches!(updated.changes.get(&key), Some(EntryModification::Update(v)) if Arc::ptr_eq(v, &value))
    );
}

#[test]
fn remove_existing_no_clear() {
    let key = Value::Int32Value(2);
    let value = Arc::new(4);

    let summary = TransactionSummary::default();
    let updated = summary.update(key.clone(), value).remove(key.clone());
    assert_eq!(updated.coordination_id, 0);
    assert!(!updated.clear);
    assert_eq!(updated.changes.len(), 1);
    assert!(matches!(
        updated.changes.get(&key),
        Some(EntryModification::Remove)
    ));
}

#[test]
fn remove_existing_clear() {
    let key = Value::Int32Value(2);
    let value = Arc::new(4);

    let summary = TransactionSummary::clear();
    let updated = summary.update(key.clone(), value).remove(key.clone());
    assert_eq!(updated.coordination_id, 0);
    assert!(updated.clear);
    assert_eq!(updated.changes.len(), 1);
    assert!(matches!(
        updated.changes.get(&key),
        Some(EntryModification::Remove)
    ));
}

#[test]
fn remove_non_existent_no_clear() {
    let key = Value::Int32Value(2);

    let summary: TransactionSummary<Value, i32> = TransactionSummary::default();
    let updated = summary.remove(key.clone());
    assert_eq!(updated.coordination_id, 0);
    assert!(!updated.clear);
    assert_eq!(updated.changes.len(), 1);
    assert!(matches!(
        updated.changes.get(&key),
        Some(EntryModification::Remove)
    ));
}

#[test]
fn remove_non_existent_clear() {
    let key = Value::Int32Value(2);

    let summary: TransactionSummary<Value, i32> = TransactionSummary::clear();
    let updated = summary.remove(key.clone());
    assert_eq!(updated.coordination_id, 0);
    assert!(updated.clear);
    assert_eq!(updated.changes.len(), 1);
    assert!(matches!(
        updated.changes.get(&key),
        Some(EntryModification::Remove)
    ));
}

#[test]
fn to_events_checkpoint() {
    let with_id: TransactionSummary<i32, String> = TransactionSummary::with_id(67);
    let events = with_id.to_events();
    assert!(matches!(events.as_slice(), [MapLaneEvent::Checkpoint(67)]));
}

#[test]
fn to_events_no_clear() {
    let key1 = Value::Int32Value(2);
    let value1 = Arc::new(4);

    let key2 = Value::Int32Value(6);

    let summary = TransactionSummary::default();
    let updated = summary.update(key1, value1.clone()).remove(key2);

    let events = updated.to_events();
    assert!(matches!(events.as_slice(),
    [MapLaneEvent::Update(Value::Int32Value(2), v), MapLaneEvent::Remove(Value::Int32Value(6))] |
    [MapLaneEvent::Remove(Value::Int32Value(6)), MapLaneEvent::Update(Value::Int32Value(2), v)]
    if Arc::ptr_eq(v, &value1)));
}

#[test]
fn to_events_clear() {
    let key1 = Value::Int32Value(2);
    let value1 = Arc::new(4);

    let key2 = Value::Int32Value(6);

    let summary = TransactionSummary::clear();
    let updated = summary.update(key1, value1.clone()).remove(key2);

    let events = updated.to_events();
    assert!(matches!(events.as_slice(),
    [MapLaneEvent::Clear, MapLaneEvent::Update(Value::Int32Value(2), v), MapLaneEvent::Remove(Value::Int32Value(6))] |
    [MapLaneEvent::Clear, MapLaneEvent::Remove(Value::Int32Value(6)), MapLaneEvent::Update(Value::Int32Value(2), v)]
    if Arc::ptr_eq(v, &value1)));
}

#[tokio::test]
async fn clear_summary_transaction() {
    let key = Value::Int32Value(2);

    let summary: TransactionSummary<Value, i32> = TransactionSummary::default();
    let updated = summary.remove(key.clone());

    let var = TVar::new(updated);

    let result = atomically(&clear_summary(&var), ExactlyOnce).await;
    assert!(result.is_ok());

    let after = var.load().await;
    let TransactionSummary {
        coordination_id,
        clear,
        changes,
    } = after.as_ref();
    assert_eq!(*coordination_id, 0);
    assert!(*clear);
    assert!(changes.is_empty());
}

#[tokio::test]
async fn update_summary_transaction() {
    let key1 = Value::Int32Value(2);
    let value1 = Arc::new(17);

    let key2 = Value::Int32Value(12);
    let value2 = Arc::new(34);

    let summary = TransactionSummary::default().update(key1.clone(), value1.clone());

    let var = TVar::new(summary);

    let result = atomically(
        &update_summary(&var, key2.clone(), value2.clone()),
        ExactlyOnce,
    )
    .await;
    assert!(result.is_ok());

    let after = var.load().await;
    let TransactionSummary {
        coordination_id,
        clear,
        changes,
    } = after.as_ref();
    assert_eq!(*coordination_id, 0);
    assert!(!*clear);
    assert_eq!(changes.len(), 2);
    assert!(
        matches!(changes.get(&key1), Some(EntryModification::Update(v)) if Arc::ptr_eq(v, &value1))
    );
    assert!(
        matches!(changes.get(&key2), Some(EntryModification::Update(v)) if Arc::ptr_eq(v, &value2))
    );
}

#[tokio::test]
async fn remove_summary_transaction() {
    let key1 = Value::Int32Value(2);
    let value1 = Arc::new(17);

    let key2 = Value::Int32Value(12);
    let value2 = Arc::new(34);

    let summary = TransactionSummary::default()
        .update(key1.clone(), value1.clone())
        .update(key2.clone(), value2.clone());

    let var = TVar::new(summary);

    let result = atomically(&remove_summary(&var, key2.clone()), ExactlyOnce).await;
    assert!(result.is_ok());

    let after = var.load().await;
    let TransactionSummary {
        coordination_id,
        clear,
        changes,
    } = after.as_ref();
    assert_eq!(*coordination_id, 0);
    assert!(!*clear);
    assert_eq!(changes.len(), 2);
    assert!(
        matches!(changes.get(&key1), Some(EntryModification::Update(v)) if Arc::ptr_eq(v, &value1))
    );
    assert!(matches!(
        changes.get(&key2),
        Some(EntryModification::Remove)
    ));
}
