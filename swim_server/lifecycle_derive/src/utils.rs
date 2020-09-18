use proc_macro::TokenStream;
use proc_macro2::Ident;

macro_rules! create_tasks_ast {

    ( $( $task:ident ), *) => {
        {
            quote! {
                vec![
                    $(
                      #$task.boxed(),
                    )*
                ];
            }
        }
    };
}

macro_rules! create_agent_ast {
    ( $agent_name:ident, $( $field:ident ), *) => {
        {
            quote! {
                #$agent_name {
                    $(
                        #$field,
                    )*
                };
            }
        }
    };
}

macro_rules! create_lifecycles_ast {
    ( $( $lifecycle: ident), *) => {{
        quote! {
            $(
                #$lifecycle
            )*
        }
    }};
}
