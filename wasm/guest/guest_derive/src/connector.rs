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

use proc_macro2::{Ident, TokenStream};
use quote::{quote, ToTokens, TokenStreamExt};
use syn::{Error, Item, ItemFn, Visibility};

use swim_utilities::errors::validation::Validation;
use swim_utilities::errors::Errors;

const PUB_CONN: &str = "Connector functions must be public.";
const CONN_ONLY_FN: &str = "The #[connector] macro can only be applied to functions.";

pub fn parse_connector(item: &Item) -> Validation<WasmConnector, Errors<Error>> {
    match item {
        Item::Fn(ItemFn { sig, vis, .. }) => match vis {
            Visibility::Public(_) => Validation::valid(WasmConnector { ident: &sig.ident }),
            _ => Validation::fail(Error::new_spanned(vis, PUB_CONN)),
        },
        item => Validation::fail(Error::new_spanned(item, CONN_ONLY_FN)),
    }
}

pub struct WasmConnector<'l> {
    ident: &'l Ident,
}

impl<'l> ToTokens for WasmConnector<'l> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let WasmConnector { ident } = self;
        tokens.append_all(quote! {
            #[no_mangle]
            #[allow(unused)]
            pub fn init_properties(ptr: *mut u8, len: usize) -> *mut ConnectorProperties {
                let input_data = unsafe { std::vec::Vec::from_raw_parts(ptr, len, len) };
                let props = bincode::deserialize(input_data.as_slice())
                    .expect("Failed to deserialize connector properties");
                Box::into_raw(Box::new(props))
            }

            #[no_mangle]
            #[allow(unused)]
            pub fn alloc(len: usize) -> *mut u8 {
                let mut buf = std::vec::Vec::with_capacity(len);
                let ptr = buf.as_mut_ptr();
                std::mem::forget(buf);
                ptr
            }

            #[no_mangle]
            #[allow(unused)]
            pub unsafe fn dealloc(ptr: *mut u8, size: usize) {
                let data = std::vec::Vec::from_raw_parts(ptr, size, size);
                std::mem::drop(data);
            }

            #[no_mangle]
            #[allow(unused)]
            pub fn dispatch(ptr: *mut u8, len: usize, props: &*mut swim_wasm_connector::ConnectorProperties) {
                let input_data = unsafe { std::vec::Vec::from_raw_parts(ptr, len, len) };
                let f: fn(swim_wasm_connector::ConnectorContext, &swim_wasm_connector::ConnectorProperties, std::vec::Vec<u8>) -> std::result::Result<(), Box<dyn std::error::Error>> = #ident;
                let properties = unsafe { &**props };
                let result = f(swim_wasm_connector::ConnectorContext, properties, input_data).map_err(|e| e.to_string());
            }
        })
    }
}
