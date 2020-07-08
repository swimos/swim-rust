use crate::model::schema::slot::SlotSchema;
use crate::model::schema::{Schema, StandardSchema};
use crate::model::{Item, ToValue, Value};
use std::cmp::Ordering;

/// Schema for Recon [`Item`]s.
#[derive(Clone, Debug, PartialEq)]
pub enum ItemSchema {
    Field(SlotSchema),
    ValueItem(StandardSchema),
}

impl PartialOrd for ItemSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else {
            match (self, other) {
                (ItemSchema::Field(this_schema), ItemSchema::Field(other_schema)) => {
                    this_schema.partial_cmp(other_schema)
                }
                (ItemSchema::ValueItem(this_schema), ItemSchema::ValueItem(other_schema)) => {
                    this_schema.partial_cmp(other_schema)
                }
                _ => None,
            }
        }
    }
}

impl ToValue for ItemSchema {
    fn to_value(&self) -> Value {
        match self {
            ItemSchema::Field(slot) => slot.to_value(),
            ItemSchema::ValueItem(s) => s.to_value(),
        }
    }
}

impl Schema<Item> for ItemSchema {
    fn matches(&self, item: &Item) -> bool {
        match self {
            ItemSchema::Field(slot_schema) => slot_schema.matches(item),
            ItemSchema::ValueItem(schema) => match item {
                Item::ValueItem(value) => schema.matches(value),
                Item::Slot(_, _) => false,
            },
        }
    }
}
