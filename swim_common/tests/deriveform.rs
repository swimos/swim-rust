use std::sync::Arc;
use swim_common::form::structural::read::parser::{parse_recognize, ParseError, Span};
use swim_common::form::structural::read::StructuralReadable;
use swim_common::form::Form;
use swim_common::model::{Item, Value};
//use swim_common::model::text::Text;

fn run_recognizer<T: StructuralReadable>(rep: &str) -> Result<T, ParseError> {
    let span = Span::new(rep);
    parse_recognize(span)
}

#[test]
pub fn map_update() {
    #[derive(Debug, PartialEq, Eq, Form, Clone)]
    enum MapUpdate<K, V> {
        #[form(tag = "update")]
        Update(#[form(header, name = "key")] K, #[form(body)] Arc<V>),
        #[form(tag = "remove")]
        Remove(#[form(header, name = "key")] K),
        #[form(tag = "clear")]
        Clear,
    }

    type Upd = MapUpdate<Value, Value>;

    //let result = run_recognizer::<Upd>("@remove(key: hello)");
    //assert_eq!(result, Ok(MapUpdate::Remove(Value::text("hello"))));
    let result = <Upd as Form>::try_from_value(&Value::of_attr((
        "remove",
        Value::record(vec![Item::slot("key", "hello")]),
    )));
    assert_eq!(result, Ok(MapUpdate::Remove(Value::text("hello"))))
}
