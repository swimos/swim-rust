use darling::{ast, FromDeriveInput, FromField, FromMeta};
use proc_macro2::{Ident, Span};
use std::num::NonZeroUsize;

#[derive(Debug, FromMeta)]
pub struct AgentAttrs {
    #[darling(map = "to_ident")]
    pub agent: Ident,
    #[darling(default = "default_on_start")]
    #[darling(map = "to_ident")]
    pub on_start: Ident,
}

#[derive(Debug, FromMeta)]
pub struct CommandAttrs {
    #[darling(map = "to_ident")]
    pub agent: Ident,
    #[darling(map = "to_ident")]
    pub command_type: Ident,
    #[darling(default = "default_on_command")]
    #[darling(map = "to_ident")]
    pub on_command: Ident,
}

#[derive(Debug, FromMeta)]
pub struct ActionAttrs {
    #[darling(map = "to_ident")]
    pub agent: Ident,
    #[darling(map = "to_ident")]
    pub command_type: Ident,
    #[darling(map = "to_ident")]
    pub response_type: Ident,
    #[darling(default = "default_on_command")]
    #[darling(map = "to_ident")]
    pub on_command: Ident,
}

#[derive(Debug, FromMeta)]
pub struct ValueAttrs {
    #[darling(map = "to_ident")]
    pub agent: Ident,
    #[darling(map = "to_ident")]
    pub event_type: Ident,
    #[darling(default = "default_on_start")]
    #[darling(map = "to_ident")]
    pub on_start: Ident,
    #[darling(default = "default_on_event")]
    #[darling(map = "to_ident")]
    pub on_event: Ident,
}

#[derive(Debug, FromMeta)]
pub struct MapAttrs {
    #[darling(map = "to_ident")]
    pub agent: Ident,
    #[darling(map = "to_ident")]
    pub key_type: Ident,
    #[darling(map = "to_ident")]
    pub value_type: Ident,
    #[darling(default = "default_on_start")]
    #[darling(map = "to_ident")]
    pub on_start: Ident,
    #[darling(default = "default_on_event")]
    #[darling(map = "to_ident")]
    pub on_event: Ident,
}

fn to_ident(value: String) -> Ident {
    Ident::new(&value, Span::call_site())
}

fn default_on_start() -> Ident {
    to_ident("on_start".to_string())
}

fn default_on_event() -> Ident {
    to_ident("on_event".to_string())
}

fn default_on_command() -> Ident {
    to_ident("on_command".to_string())
}

#[derive(Debug, FromDeriveInput)]
pub struct SwimAgent {
    pub ident: syn::Ident,
    pub data: ast::Data<(), LifecycleAttrs>,
}

#[derive(Debug, FromField)]
#[darling(attributes(lifecycle))]
pub struct LifecycleAttrs {
    pub ident: Option<syn::Ident>,
    pub ty: syn::Type,

    #[darling(default)]
    pub name: Option<String>,
}
