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

use proc_macro2::Ident;
use quote::{quote, ToTokens, TokenStreamExt};
use swim_utilities::errors::validation::Validation;
use swim_utilities::errors::Errors;
use syn::{Error, Item, ItemStruct};

const WASM_ONLY_STRUCTS: &str =
    "The #[wasm_agent] macro can only be applied to struct definitions.";

pub fn parse_agent(item: &Item) -> Validation<WasmAgent, Errors<Error>> {
    match item {
        Item::Struct(ItemStruct { ident, .. }) => Validation::valid(WasmAgent { ident }),
        item => Validation::fail(Error::new_spanned(item, WASM_ONLY_STRUCTS)),
    }
}

pub struct WasmAgent<'l> {
    ident: &'l Ident,
}

impl<'l> ToTokens for WasmAgent<'l> {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let WasmAgent { ident } = self;

        let init_fn = InitFn::new(ident);

        tokens.append_all(quote! {
            use swim_wasm_guest::prelude::host::WasmHostAccess;
            use swim_wasm_guest::prelude::agent::{wasm_agent, Dispatcher};

            extern "C" {
                fn copy_schema(ptr: i32, len: i32);
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

            #[allow(unused)]
            #[no_mangle]
            pub fn dispatch(mut dispatcher: &*mut dyn Dispatcher, ptr: *mut u8, len: usize) {
                let input_data = unsafe { std::vec::Vec::from_raw_parts(ptr, len, len) };
                unsafe {
                    (**dispatcher).dispatch(input_data);
                }
            }

            #[allow(unused)]
            #[no_mangle]
            pub fn stop(mut dispatcher: std::boxed::Box<*mut dyn Dispatcher>) {
                unsafe { std::boxed::Box::from_raw(*dispatcher) };
            }

            #init_fn
        })
    }
}

struct InitFn<'l> {
    ident: &'l Ident,
}

impl<'l> InitFn<'l> {
    pub fn new(ident: &'l Ident) -> InitFn<'l> {
        InitFn { ident }
    }
}

impl<'l> ToTokens for InitFn<'l> {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let InitFn { ident } = self;

        tokens.append_all(quote! {
            #[allow(unused)]
            #[no_mangle]
            pub fn init() -> std::boxed::Box<*mut dyn Dispatcher> {
                let (spec, agent) = unsafe { wasm_agent::<#ident>(WasmHostAccess::default()) };

                let mut buf = swim_wasm_guest::prelude::bincode::serialize(&spec).expect("Serializing should be infallible");

                let ptr = buf.as_mut_ptr();
                let out_len = buf.len();
                std::mem::forget(buf);

                unsafe {
                    copy_schema(ptr as i32, out_len as i32);
                }

                agent
            }
        })
    }
}
