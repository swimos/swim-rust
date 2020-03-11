use common::model::{Item, Value};
use common::structure::form::FormParseErr;

#[cfg(test)]
mod tests;

pub trait Form: Sized {
    fn try_into_value(&self) -> Result<Value, FormParseErr>;
    fn try_from_value(value: &Value) -> Result<Self, FormParseErr>;

    // Reserved for macro implementation.
    // This should never be implemented by hand.
    #[doc(hidden)]
    fn __assert_receiver_is_total_form(&self) {}
}

impl Form for f64 {
    fn try_into_value(&self) -> Result<Value, FormParseErr> {
        Ok(Value::Float64Value(*self))
    }

    fn try_from_value(value: &Value) -> Result<Self, FormParseErr> {
        match value {
            Value::Float64Value(i) => Ok(*i),
            v => Err(FormParseErr::IncorrectType(v.to_owned())),
        }
    }
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

impl Form for i64 {
    fn try_into_value(&self) -> Result<Value, FormParseErr> {
        Ok(Value::Int64Value(*self))
    }

    fn try_from_value(value: &Value) -> Result<Self, FormParseErr> {
        match value {
            Value::Int64Value(i) => Ok(*i),
            v => Err(FormParseErr::IncorrectType(v.to_owned())),
        }
    }
}

impl Form for bool {
    fn try_into_value(&self) -> Result<Value, FormParseErr> {
        Ok(Value::BooleanValue(*self))
    }

    fn try_from_value(value: &Value) -> Result<Self, FormParseErr> {
        match value {
            Value::BooleanValue(i) => Ok(*i),
            v => Err(FormParseErr::IncorrectType(v.to_owned())),
        }
    }
}

impl Form for String {
    fn try_into_value(&self) -> Result<Value, FormParseErr> {
        Ok(Value::Text(String::from(self)))
    }

    fn try_from_value(value: &Value) -> Result<Self, FormParseErr> {
        match value {
            Value::Text(i) => Ok(i.to_owned()),
            v => Err(FormParseErr::IncorrectType(v.to_owned())),
        }
    }
}

impl<T: Form> Form for Vec<T> {
    fn try_into_value(&self) -> Result<Value, FormParseErr> {
        unimplemented!()
    }

    fn try_from_value(value: &Value) -> Result<Self, FormParseErr> {
        match value {
            Value::Record(attr, items) if attr.is_empty() => {
                let length = items.len();
                items.into_iter().try_fold(
                    Vec::with_capacity(length),
                    |mut results: Vec<T>, item| match item {
                        Item::ValueItem(v) => {
                            let result = T::try_from_value(v)?;
                            results.push(result);
                            Ok(results)
                        }
                        i => Err(FormParseErr::IllegalItem(i.to_owned())),
                    },
                )
            }
            v => Err(FormParseErr::IncorrectType(v.to_owned())),
        }
    }
}