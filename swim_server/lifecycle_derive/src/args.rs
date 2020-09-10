use darling::FromMeta;

#[derive(Debug, FromMeta)]
pub struct AgentAttrs {
    pub agent: String,
    #[darling(default = "default_on_start")]
    pub on_start: String,
}

#[derive(Debug, FromMeta)]
pub struct CommandAttrs {
    pub agent: String,
    pub command_type: String,
    #[darling(default = "default_on_command")]
    pub on_command: String,
}

#[derive(Debug, FromMeta)]
pub struct ActionAttrs {
    pub agent: String,
    pub command_type: String,
    pub response_type: String,
    #[darling(default = "default_on_command")]
    pub on_command: String,
}

#[derive(Debug, FromMeta)]
pub struct ValueAttrs {
    pub agent: String,
    pub event_type: String,
    #[darling(default = "default_on_start")]
    pub on_start: String,
    #[darling(default = "default_on_event")]
    pub on_event: String,
}

#[derive(Debug, FromMeta)]
pub struct MapAttrs {
    pub agent: String,
    pub key_type: String,
    pub value_type: String,
    #[darling(default = "default_on_start")]
    pub on_start: String,
    #[darling(default = "default_on_event")]
    pub on_event: String,
}

fn default_on_start() -> String {
    "on_start".to_string()
}

fn default_on_event() -> String {
    "on_event".to_string()
}

fn default_on_command() -> String {
    "on_command".to_string()
}
