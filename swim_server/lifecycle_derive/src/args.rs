use darling::{ast, FromDeriveInput, FromField, FromMeta};
use proc_macro2::{Ident, Span};
use syn::{Path, Type, TypePath};

const COMMAND_LANE: &str = "CommandLane";
const VALUE_LANE: &str = "ValueLane";
const MAP_LANE: &str = "MapLane";

#[derive(Debug, FromMeta)]
pub(crate) struct AgentAttrs {
    #[darling(map = "to_ident")]
    pub(crate) agent: Ident,
    #[darling(default = "default_on_start")]
    #[darling(map = "to_ident")]
    pub(crate) on_start: Ident,
}

#[derive(Debug, FromMeta)]
pub(crate) struct CommandAttrs {
    #[darling(map = "to_ident")]
    pub(crate) agent: Ident,
    #[darling(map = "to_ident")]
    pub(crate) command_type: Ident,
    #[darling(default = "default_on_command")]
    #[darling(map = "to_ident")]
    pub(crate) on_command: Ident,
}

#[derive(Debug, FromMeta)]
pub(crate) struct ActionAttrs {
    #[darling(map = "to_ident")]
    pub(crate) agent: Ident,
    #[darling(map = "to_ident")]
    pub(crate) command_type: Ident,
    #[darling(map = "to_ident")]
    pub(crate) response_type: Ident,
    #[darling(default = "default_on_command")]
    #[darling(map = "to_ident")]
    pub(crate) on_command: Ident,
}

#[derive(Debug, FromMeta)]
pub(crate) struct ValueAttrs {
    #[darling(map = "to_ident")]
    pub(crate) agent: Ident,
    #[darling(map = "to_ident")]
    pub(crate) event_type: Ident,
    #[darling(default = "default_on_start")]
    #[darling(map = "to_ident")]
    pub(crate) on_start: Ident,
    #[darling(default = "default_on_event")]
    #[darling(map = "to_ident")]
    pub(crate) on_event: Ident,
}

#[derive(Debug, FromMeta)]
pub(crate) struct MapAttrs {
    #[darling(map = "to_ident")]
    pub(crate) agent: Ident,
    #[darling(map = "to_ident")]
    pub(crate) key_type: Ident,
    #[darling(map = "to_ident")]
    pub(crate) value_type: Ident,
    #[darling(default = "default_on_start")]
    #[darling(map = "to_ident")]
    pub(crate) on_start: Ident,
    #[darling(default = "default_on_event")]
    #[darling(map = "to_ident")]
    pub(crate) on_event: Ident,
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
#[darling(attributes(agent))]
pub(crate) struct SwimAgent {
    pub(crate) ident: syn::Ident,
    pub(crate) data: ast::Data<(), LifecycleAttrs>,
    #[darling(map = "to_ident")]
    pub(crate) config: Ident,
}

#[derive(Debug, FromField)]
#[darling(attributes(lifecycle))]
pub(crate) struct LifecycleAttrs {
    pub(crate) ident: Option<syn::Ident>,
    pub(crate) ty: syn::Type,
    pub(crate) name: Option<String>,
}

impl LifecycleAttrs {
    pub(crate) fn get_lane_type(&self) -> Option<LaneType> {
        if let Type::Path(TypePath {
            path: Path { segments, .. },
            ..
        }) = &self.ty
        {
            if let Some(path_segment) = segments.last() {
                return match path_segment.ident.to_string().as_str() {
                    COMMAND_LANE => Some(LaneType::Command),
                    VALUE_LANE => Some(LaneType::Value),
                    MAP_LANE => Some(LaneType::Map),
                    _ => None,
                };
            }
        }

        None
    }
}

//Todo add action lane
pub(crate) enum LaneType {
    Command,
    Value,
    Map,
}
