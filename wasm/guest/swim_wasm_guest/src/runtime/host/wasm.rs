// Copyright 2015-2023 Swim Inc.
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

use std::io::Cursor;
use std::mem::{forget, size_of};
use std::ptr::slice_from_raw_parts;

use byteorder::ReadBytesExt;
use serde::de::DeserializeOwned;
use serde::Serialize;

use wasm_ir::wpc::EnvAccess;

const LEN_SIZE: usize = size_of::<u32>();

// todo: experiment with this not flushing events to the host immediately. i.e, stack the events
//  and when either:
//  - a dispatch event is fired that expects a return type, flush all of the events in one go and await the response
//  - the size of the pending events reaches a certain limit
//  - the struct is dropped
#[derive(Debug, Clone, Default)]
pub struct WasmHostAccess;

impl EnvAccess for WasmHostAccess {
    fn dispatch<T, R>(&self, item: T) -> R
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        let mut buf = bincode::serialize(&item).expect("Serializing should be infallible");

        let ptr = buf.as_mut_ptr();
        let out_len = buf.len();
        forget(buf);

        extern "C" {
            fn host_call(ptr: i32, len: i32) -> *mut u8;
        }

        let response_ptr = unsafe { host_call(ptr as i32, out_len as i32) };

        let len_slice = slice_from_raw_parts(response_ptr, LEN_SIZE);
        let len = unsafe { (&*len_slice).read_u32::<byteorder::NativeEndian>().unwrap() };
        let base = unsafe { response_ptr.add(LEN_SIZE) };

        let response = unsafe { Vec::from_raw_parts(base, len as usize, len as usize) };

        bincode::deserialize_from(Cursor::new(response))
            .expect("Deserializing should be infallible")
    }
}
