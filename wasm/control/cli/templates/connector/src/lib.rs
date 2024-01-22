use std::error::Error;
use swim_wasm_connector::*;

#[connector]
pub fn on_message(context: &mut ConnectorContext, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
    context.send(data, "node_uri", "lane_uri");
    Ok(())
}
