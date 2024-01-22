use byteorder::ReadBytesExt;
use std::error::Error;
use swim_wasm_connector::*;

#[connector]
pub fn on_message(context: &mut ConnectorContext, _: Vec<u8>) -> Result<(), Box<dyn Error>> {
    let node = context.get_property("node").unwrap();
    let counter = context.get_property("lane").unwrap();

    let (state, event_count) = match context.state().read_u32::<byteorder::NativeEndian>() {
        Ok(mut count) => {
            count += 1;
            (count.to_ne_bytes().to_vec(), count)
        }
        Err(_) => (1u32.to_ne_bytes().to_vec(), 1),
    };

    context.replace_state(state);
    context.send(&node, &counter, &event_count);
    Ok(())
}
