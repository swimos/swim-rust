use darling::{ast, FromDeriveInput, FromField, FromMeta};
use proc_macro2::{Delimiter, Group, Ident, Span};
use quote::ToTokens;
use syn::export::TokenStream2;
use syn::{Path, Type, TypePath};
const COMMAND_LANE: &str = "CommandLane";
const ACTION_LANE: &str = "ActionLane";
const VALUE_LANE: &str = "ValueLane";
const MAP_LANE: &str = "MapLane";

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

fn default_config() -> ConfigType {
    ConfigType::Unit
}

fn parse_config(value: String) -> ConfigType {
    ConfigType::Struct(to_ident(value))
}

#[derive(Debug)]
pub enum ConfigType {
    Struct(Ident),
    Unit,
}

impl ToTokens for ConfigType {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        match self {
            ConfigType::Struct(ident) => ident.to_tokens(tokens),
            ConfigType::Unit => {
                Group::new(Delimiter::Parenthesis, TokenStream2::new()).to_tokens(tokens)
            }
        }
    }
}

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(agent))]
pub struct SwimAgentAttrs {
    pub ident: syn::Ident,
    #[darling(default = "default_config")]
    #[darling(map = "parse_config")]
    pub config: ConfigType,
    pub data: ast::Data<(), LifecycleAttrs>,
    pub generics: syn::Generics,
}

#[derive(Debug, FromField)]
#[darling(attributes(lifecycle))]
pub struct LifecycleAttrs {
    pub ident: Option<syn::Ident>,
    pub ty: syn::Type,
    #[darling(default)]
    pub public: bool,
    pub name: Option<String>,
}

impl LifecycleAttrs {
    pub fn get_lane_type(&self) -> Option<LaneType> {
        if let Type::Path(TypePath {
            path: Path { segments, .. },
            ..
        }) = &self.ty
        {
            if let Some(path_segment) = segments.last() {
                return match path_segment.ident.to_string().as_str() {
                    COMMAND_LANE => Some(LaneType::Command),
                    ACTION_LANE => Some(LaneType::Action),
                    VALUE_LANE => Some(LaneType::Value),
                    MAP_LANE => Some(LaneType::Map),
                    _ => None,
                };
            }
        }

        None
    }
}

pub enum LaneType {
    Command,
    Action,
    Value,
    Map,
}
