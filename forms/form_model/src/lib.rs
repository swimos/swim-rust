// #![feature(rustc_attrs)]

use common::model::Value;
use common::structure::form::FormParseErr;

pub trait Form: Sized {
    fn try_into_value(&self) -> Result<Value, FormParseErr>;
    fn try_from_value(value: &Value) -> Result<Self, FormParseErr>;

    // Reserved for macro implementation.
    // This should never be implemented by hand.
    #[doc(hidden)]
    fn __assert_receiver_is_total_form(&self) {}
}

impl Form for i32 {
    fn try_into_value(&self) -> Result<Value, FormParseErr> {
        Ok(Value::Int32Value(*self))
    }

    fn try_from_value(value: &Value) -> Result<Self, FormParseErr> {
        match value {
            Value::Int32Value(i) => Ok(*i),
            v => Err(FormParseErr::IncorrectType(v.to_owned())),
        }
    }
}
