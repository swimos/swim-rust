use serde_json::Value;
use std::error::Error;

use swim_wasm_connector::*;

#[connector]
pub fn on_message(context: &mut ConnectorContext, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
    let node = context.get_property("node").unwrap();
    let lane = context.get_property("lane").unwrap();
    let extract = context.get_property("extract").unwrap();

    match serde_json::from_slice::<Value>(data.as_slice()) {
        Ok(Value::Object(map)) => match map.get(&extract).unwrap() {
            Value::Number(v) => {
                context.send(&node, &lane, &v.as_u64());
            }
            Value::String(v) => context.send(&node, &lane, v),
            _ => panic!(),
        },
        _ => panic!("Malformed message"),
    }

    Ok(())
}
