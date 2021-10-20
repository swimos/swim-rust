use crate::StoreError;
use serde::{Deserialize, Serialize};

pub fn serialize_then<S, F, O, E>(engine: &E, obj: &S, f: F) -> Result<O, StoreError>
where
    S: Serialize,
    F: Fn(&E, Vec<u8>) -> Result<O, StoreError>,
{
    f(engine, serialize(obj)?).map_err(Into::into)
}

pub fn serialize<S: Serialize>(obj: &S) -> Result<Vec<u8>, StoreError> {
    bincode::serialize(obj).map_err(|e| StoreError::Encoding(e.to_string()))
}

pub fn deserialize<'de, D: Deserialize<'de>>(obj: &'de [u8]) -> Result<D, StoreError> {
    bincode::deserialize(obj).map_err(|e| StoreError::Decoding(e.to_string()))
}

pub fn deserialize_key<B: AsRef<[u8]>>(bytes: B) -> Result<u64, StoreError> {
    bincode::deserialize::<u64>(bytes.as_ref()).map_err(|e| StoreError::Decoding(e.to_string()))
}
