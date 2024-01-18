use proc_macro::TokenStream;

use proc_macro2::Ident;
use quote::{quote, ToTokens, TokenStreamExt};
use syn::{parse_macro_input, Item, ItemStruct};

use macro_utilities::to_compile_errors;
use swim_utilities::errors::validation::Validation;
use swim_utilities::errors::Errors;

const ONLY_STRUCTS: &str = "The #[wasm_agent] macro can only be applied to struct definitions.";
const NO_ARGS: &str = "#[wasm_agent] does not accept any attribute arguments";

#[proc_macro_attribute]
pub fn wasm_agent(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item: Item = parse_macro_input!(item as Item);
    validate_attr(attr)
        .and_then(|_| parse_agent(&item))
        .into_result()
        .map(|proj| {
            quote! {
                #item
                #proj
            }
        })
        .unwrap_or_else(|errs| to_compile_errors(errs.into_vec()))
        .into()
}

fn validate_attr(attr: TokenStream) -> Validation<(), Errors<syn::Error>> {
    let tokens: proc_macro2::TokenStream = attr.into();

    if tokens.is_empty() {
        Validation::valid(())
    } else {
        Validation::fail(Errors::of(syn::Error::new_spanned(tokens, NO_ARGS)))
    }
}

fn parse_agent(item: &Item) -> Validation<WasmAgent, Errors<syn::Error>> {
    match item {
        Item::Struct(ItemStruct { ident, .. }) => Validation::valid(WasmAgent { ident }),
        item => Validation::fail(syn::Error::new_spanned(item, ONLY_STRUCTS)),
    }
}

struct WasmAgent<'l> {
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

                let mut buf = bincode::serialize(&spec).expect("Serializing should be infallible");

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
