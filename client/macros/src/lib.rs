#[allow(unused_extern_crates)]
extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_attribute]
pub fn client(args: TokenStream, item: TokenStream) -> TokenStream {
    build_client(args, item)
}

fn build_client(args: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemFn);
    let args = syn::parse_macro_input!(args as syn::AttributeArgs);

    if !input.sig.inputs.is_empty() {
        let msg = "Swim clients cannot accept arguments";
        return syn::Error::new_spanned(&input.sig.inputs, msg)
            .to_compile_error()
            .into();
    }

    build(input, args).unwrap_or_else(|e| e.to_compile_error().into())
}

fn build(mut input: syn::ItemFn, _args: syn::AttributeArgs) -> Result<TokenStream, syn::Error> {
    let sig = &mut input.sig;
    let body = &input.block;
    let _attrs = &input.attrs;
    let vis = input.vis;

    if sig.asyncness.is_none() {
        let msg = "Function signature must be async";
        return Err(syn::Error::new_spanned(sig.fn_token, msg));
    }

    sig.asyncness = None;

    // todo: Allow for attributes to set core threads etc.
    let runtime = quote! { tokio::runtime::Builder::new().basic_scheduler().threaded_scheduler() };

    let result = quote! {
        #vis #sig {
            let mut runtime = #runtime
                .enable_all()
                .build()
                .expect("Failed to build Tokio runtime.");

            runtime.block_on(async {
                swim::interface::SwimContext::enter();
            });

            runtime.block_on(async { #body })
        }
    };

    Ok(result.into())
}
