use common::model::{Item, Value};
use deserialize::FormDeserializeErr;
use serialize::FormSerializeErr;

#[cfg(test)]
mod tests;

pub trait Form: Sized {
    fn try_into_value(&self) -> Result<Value, FormSerializeErr>;
    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr>;

    fn __assert_receiver_is_total_form(&self) {}
}

impl Form for f64 {
    fn try_into_value(&self) -> Result<Value, FormSerializeErr> {
        Ok(Value::Float64Value(*self))
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Float64Value(i) => Ok(*i),
            v => Err(FormDeserializeErr::IncorrectType(v.to_owned())),
        }
    }
}

impl Form for i32 {
    fn try_into_value(&self) -> Result<Value, FormSerializeErr> {
        Ok(Value::Int32Value(*self))
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Int32Value(i) => Ok(*i),
            v => Err(FormDeserializeErr::IncorrectType(v.to_owned())),
        }
    }
}

impl Form for i64 {
    fn try_into_value(&self) -> Result<Value, FormSerializeErr> {
        Ok(Value::Int64Value(*self))
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Int64Value(i) => Ok(*i),
            v => Err(FormDeserializeErr::IncorrectType(v.to_owned())),
        }
    }
}

impl Form for bool {
    fn try_into_value(&self) -> Result<Value, FormSerializeErr> {
        Ok(Value::BooleanValue(*self))
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::BooleanValue(i) => Ok(*i),
            v => Err(FormDeserializeErr::IncorrectType(v.to_owned())),
        }
    }
}

impl Form for String {
    fn try_into_value(&self) -> Result<Value, FormSerializeErr> {
        Ok(Value::Text(String::from(self)))
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Text(i) => Ok(i.to_owned()),
            v => Err(FormDeserializeErr::IncorrectType(v.to_owned())),
        }
    }
}

impl<T: Form> Form for Vec<T> {
    fn try_into_value(&self) -> Result<Value, FormSerializeErr> {
        unimplemented!()
    }

    fn try_from_value(value: &Value) -> Result<Self, FormDeserializeErr> {
        match value {
            Value::Record(attr, items) if attr.is_empty() => {
                let length = items.len();
                items
                    .iter()
                    .try_fold(
                        Vec::with_capacity(length),
                        |mut results: Vec<T>, item| match item {
                            Item::ValueItem(v) => {
                                let result = T::try_from_value(v)?;
                                results.push(result);
                                Ok(results)
                            }
                            i => Err(FormDeserializeErr::IllegalItem(i.to_owned())),
                        },
                    )
            }
            v => Err(FormDeserializeErr::IncorrectType(v.to_owned())),
        }
    }
}
