// Copyright 2015-2021 Swim Inc.
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

use std::collections::{HashSet, BTreeMap};

use swim_utilities::errors::{
    validation::{Validation, ValidationItExt},
    Errors,
};
use syn::{
    Attribute, FnArg, GenericParam, Ident, ImplItem, ImplItemMethod, Item, Lit, Meta, NestedMeta,
    Path, ReturnType, Signature, Type, TypeReference,
};

const NOT_IMPL: &str = "The lifecycle annotation can only be applied to an impl block.";
const NO_GENERICS: &str = "Generic lifecycles are not yet supported.";
const INCONSISTENT_HANDLERS: &str = "Method marked with inconsistent handler attributes.";

pub fn validate_input<'a>(
    agent_type: &'a Path,
    item: &'a mut Item,
) -> Validation<AgentLifecycleDescriptor<'a>, Errors<syn::Error>> {
    if let Item::Impl(block) = item {
        if !block.generics.params.is_empty() {
            return Validation::fail(syn::Error::new_spanned(block, NO_GENERICS));
        }
        let init = AgentLifecycleDescriptor::new(agent_type, &block.self_ty);
        block
            .items
            .iter_mut()
            .filter_map(|item| {
                if let ImplItem::Method(method) = item {
                    let (handler_attrs, others) =
                        method.attrs.drain(0..).partition::<Vec<_>, _>(assess_attr);
                    method.attrs.extend(others.into_iter());
                    Some((handler_attrs, &*method))
                } else {
                    None
                }
            })
            .validate_fold(Validation::valid(init), false, |acc, (attrs, method)| {
                validate_method(acc, attrs, method)
            })
    } else {
        Validation::fail(syn::Error::new_spanned(item, NOT_IMPL))
    }
}

struct Descriptor {
    kind: HandlerKind,
    targets: HashSet<String>,
}

impl Descriptor {
    fn new(kind: HandlerKind) -> Self {
        Descriptor {
            kind,
            targets: Default::default(),
        }
    }
}

fn validate_method<'a>(
    acc: AgentLifecycleDescriptor<'a>,
    attrs: Vec<Attribute>,
    method: &'a ImplItemMethod,
) -> Validation<AgentLifecycleDescriptor<'a>, Errors<syn::Error>> {
    let method_desc = attrs
        .iter()
        .append_fold(Validation::valid(None), true, |acc, attr| {
            match (acc, get_kind(attr)) {
                (Some(mut desc), Some(Ok((k, target)))) => {
                    let Descriptor { kind, targets } = &mut desc;
                    if k != *kind {
                        Validation::Failed(Some(syn::Error::new_spanned(
                            method,
                            INCONSISTENT_HANDLERS,
                        )))
                    } else {
                        if let Some(t) = target {
                            targets.insert(t);
                        }
                        Validation::valid(Some(desc))
                    }
                }
                (_, Some(Ok((kind, target)))) => {
                    let mut desc = Descriptor::new(kind);
                    if let Some(t) = target {
                        desc.targets.insert(t);
                    }
                    Validation::valid(Some(desc))
                }
                (acc, Some(Err(e))) => Validation::Validated(acc, Some(e)),
                (acc, _) => Validation::valid(acc),
            }
        });
    method_desc.and_then(move |desc| {
        if let Some(desc) = desc {
            validate_method_as(acc, desc, method)
        } else {
            Validation::valid(acc)
        }
    })
}

fn validate_method_as<'a>(
    acc: AgentLifecycleDescriptor<'a>,
    descriptor: Descriptor,
    method: &'a ImplItemMethod,
) -> Validation<AgentLifecycleDescriptor<'a>, Errors<syn::Error>> {
    let acc = Validation::valid(acc);
    let Descriptor { kind, targets } = descriptor;
    let sig = &method.sig;
    if let Err(e) = check_sig_common(sig) {
        Validation::fail(e)
    } else {
        match kind {
            HandlerKind::Start => {
                Validation::join(acc, validate_no_type_sig(sig)).and_then(|(mut acc, _)| {
                    if let Err(e) = acc.add_on_start(&sig.ident) {
                        Validation::Validated(acc, Errors::of(e))
                    } else {
                        Validation::valid(acc)
                    }
                })
            }
            HandlerKind::Stop => {
                Validation::join(acc, validate_no_type_sig(sig)).and_then(|(mut acc, _)| {
                    if let Err(e) = acc.add_on_stop(&sig.ident) {
                        Validation::Validated(acc, Errors::of(e))
                    } else {
                        Validation::valid(acc)
                    }
                })
            }
            HandlerKind::Command => Validation::join(acc, validate_typed_sig(sig, 1, true))
                .and_then(|(mut acc, t)| {
                    for target in targets {
                        if let Err(e) = acc.add_on_command(target, t, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                }),
            HandlerKind::Event => {
                Validation::join(acc, validate_typed_sig(sig, 1, true)).and_then(|(mut acc, t)| {
                    for target in targets {
                        if let Err(e) = acc.add_on_event(target, t, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                })
            }
            HandlerKind::Set => {
                Validation::join(acc, validate_typed_sig(sig, 2, true)).and_then(|(mut acc, t)| {
                    for target in targets {
                        if let Err(e) = acc.add_on_set(target, t, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                })
            }
            HandlerKind::Update => Validation::join(acc, validate_typed_sig(sig, 3, true))
                .and_then(|(mut acc, t)| {
                    for target in targets {
                        if let Err(e) = acc.add_on_update(target, t, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                }),
            HandlerKind::Remove => Validation::join(acc, validate_typed_sig(sig, 3, true))
                .and_then(|(mut acc, t)| {
                    for target in targets {
                        if let Err(e) = acc.add_on_remove(target, t, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                }),
            HandlerKind::Clear => Validation::join(acc, validate_typed_sig(sig, 1, false))
                .and_then(|(mut acc, t)| {
                    for target in targets {
                        if let Err(e) = acc.add_on_clear(target, t, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                }),
        }
    }
}

const NO_ASYNC: &str = "Event handlers cannot be async.";
const NO_UNSAFE: &str = "Event handlers cannot be unsafe.";
const MANDATORY_RETURN: &str = "Event handler methods must return an event handler.";
const ONLY_LIFETIMES: &str = "Event handlers can only have lifetime parametrs.";

fn check_sig_common(sig: &Signature) -> Result<(), syn::Error> {
    if sig.asyncness.is_some() {
        Err(NO_ASYNC)
    } else if sig.unsafety.is_some() {
        Err(NO_UNSAFE)
    } else if sig.output == ReturnType::Default {
        Err(MANDATORY_RETURN)
    } else if !sig
        .generics
        .params
        .iter()
        .all(|p| matches!(p, GenericParam::Lifetime(_)))
    {
        Err(ONLY_LIFETIMES)
    } else {
        Ok(())
    }
    .map_err(|msg| syn::Error::new_spanned(sig, msg))
}

const MANDATORY_SELF: &str = "The receiver of an event handler must be &self.";
const REQUIRED_CONTEXT: &str = "A HandlerContext parameter is required.";
const BAD_SIGNATURE: &str =
    "The handler does not have the correct signature for the annotated handler kind.";
const BAD_PARAMS: &str = "Invalid parameters to method handler annotation.";

fn validate_no_type_sig(sig: &Signature) -> Validation<(), Errors<syn::Error>> {
    let iter = sig.inputs.iter();
    check_receiver(sig, iter)
        .and_then(|mut iter| {
            if iter.next().is_none() {
                Validation::fail(syn::Error::new_spanned(sig, REQUIRED_CONTEXT))
            } else {
                Validation::valid(iter)
            }
        })
        .and_then(|iter| {
            let param_types = extract_types(iter);
            if param_types.len() == 1 {
                Validation::valid(())
            } else {
                Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE))
            }
        })
}

fn validate_typed_sig(
    sig: &Signature,
    expected_params: usize,
    peel_ref: bool,
) -> Validation<&Type, Errors<syn::Error>> {
    let iter = sig.inputs.iter();
    check_receiver(sig, iter)
        .and_then(|mut iter| {
            if iter.next().is_none() {
                Validation::fail(syn::Error::new_spanned(sig, REQUIRED_CONTEXT))
            } else {
                Validation::valid(iter)
            }
        })
        .and_then(|iter| {
            let param_types = extract_types(iter);
            if param_types.len() != expected_params + 1 {
                Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE))
            } else {
                let rep_type = param_types[1];
                if peel_ref {
                    if let Type::Reference(ref_type) = rep_type {
                        peel_ref_type(sig, ref_type)
                    } else {
                        Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE))
                    }
                } else {
                    Validation::valid(rep_type)
                }
            }
        })
}

fn peel_ref_type<'a>(
    sig: &'a Signature,
    ref_type: &'a TypeReference,
) -> Validation<&'a Type, Errors<syn::Error>> {
    if ref_type.mutability.is_some() {
        Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE))
    } else {
        Validation::valid(&*ref_type.elem)
    }
}

fn check_receiver<'a, I: Iterator<Item = &'a FnArg> + 'a>(
    sig: &'a Signature,
    mut iter: I,
) -> Validation<I, Errors<syn::Error>> {
    match iter.next() {
        Some(FnArg::Receiver(rec)) if rec.reference.is_some() && rec.mutability.is_none() => {
            Validation::valid(iter)
        }
        Some(_) => Validation::Validated(
            iter,
            Errors::of(syn::Error::new_spanned(sig, MANDATORY_SELF)),
        ),
        _ => Validation::fail(syn::Error::new_spanned(sig, MANDATORY_SELF)),
    }
}

fn extract_types<'a, I: Iterator<Item = &'a FnArg> + 'a>(iter: I) -> Vec<&'a Type> {
    iter.filter_map(|arg| match arg {
        FnArg::Typed(pat) => Some(&*pat.ty),
        _ => None,
    })
    .collect::<Vec<_>>()
}

fn get_kind(attr: &Attribute) -> Option<Result<(HandlerKind, Option<String>), syn::Error>> {
    if let Some(seg) = attr.path.segments.first() {
        let kind_str = seg.ident.to_string();
        let kind = match kind_str.as_str() {
            ON_START => Some(HandlerKind::Start),
            ON_STOP => Some(HandlerKind::Stop),
            ON_COMMAND => Some(HandlerKind::Command),
            ON_EVENT => Some(HandlerKind::Event),
            ON_SET => Some(HandlerKind::Set),
            ON_UPDATE => Some(HandlerKind::Update),
            ON_REMOVE => Some(HandlerKind::Remove),
            ON_CLEAR => Some(HandlerKind::Clear),
            _ => None,
        };
        kind.map(|k| match &k {
            HandlerKind::Start | HandlerKind::Stop => {
                if attr.tokens.is_empty() {
                    Ok((k, None))
                } else {
                    Err(syn::Error::new_spanned(attr, BAD_PARAMS))
                }
            }
            _ => extract_target(attr).map(move |target| (k, Some(target))),
        })
    } else {
        None
    }
}

fn extract_target(attr: &Attribute) -> Result<String, syn::Error> {
    let meta = attr.parse_meta()?;
    match meta {
        Meta::List(lst) => match lst.nested.iter().next() {
            Some(NestedMeta::Lit(Lit::Str(name))) if lst.nested.len() == 1 => Ok(name.value()),
            _ => Err(syn::Error::new_spanned(attr, BAD_PARAMS)),
        },
        _ => Err(syn::Error::new_spanned(attr, BAD_PARAMS)),
    }
}

#[derive(PartialEq, Eq)]
enum HandlerKind {
    Start,
    Stop,
    Command,
    Event,
    Set,
    Update,
    Remove,
    Clear,
}

const ON_START: &str = "on_start";
const ON_STOP: &str = "on_stop";
const ON_COMMAND: &str = "on_command";
const ON_EVENT: &str = "on_event";
const ON_SET: &str = "on_set";
const ON_UPDATE: &str = "on_update";
const ON_REMOVE: &str = "on_remove";
const ON_CLEAR: &str = "on_clear";

fn assess_attr(attr: &Attribute) -> bool {
    let path = &attr.path;
    if path.leading_colon.is_some() {
        false
    } else {
        match path.segments.first() {
            Some(seg) if path.segments.len() == 1 && seg.arguments.is_empty() => {
                let seg_str = seg.ident.to_string();
                match seg_str.as_str() {
                    ON_START | ON_STOP | ON_COMMAND | ON_EVENT | ON_SET | ON_UPDATE | ON_REMOVE
                    | ON_CLEAR => true,
                    _ => false,
                }
            }
            _ => false,
        }
    }
}

pub struct AgentLifecycleDescriptor<'a> {
    pub agent_type: &'a Path,
    pub lifecycle_type: &'a Type,
    pub on_start: Option<&'a Ident>,
    pub on_stop: Option<&'a Ident>,
    pub lane_lifecycles: BTreeMap<String, LaneLifecycle<'a>>,
}

const DUPLICATE_ON_STOP: &str = "Duplicate on_stop event handler.";
const DUPLICATE_ON_START: &str = "Duplicate on_start event handler.";

impl<'a> AgentLifecycleDescriptor<'a> {
    pub fn new(agent_type: &'a Path, lifecycle_type: &'a Type) -> Self {
        AgentLifecycleDescriptor {
            agent_type,
            lifecycle_type,
            on_start: None,
            on_stop: None,
            lane_lifecycles: BTreeMap::new(),
        }
    }

    pub fn add_on_stop(&mut self, method: &'a Ident) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptor { on_stop, .. } = self;
        if on_stop.is_some() {
            Err(syn::Error::new_spanned(method, DUPLICATE_ON_STOP))
        } else {
            *on_stop = Some(method);
            Ok(())
        }
    }

    pub fn add_on_start(&mut self, method: &'a Ident) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptor { on_start, .. } = self;
        if on_start.is_some() {
            Err(syn::Error::new_spanned(method, DUPLICATE_ON_START))
        } else {
            *on_start = Some(method);
            Ok(())
        }
    }

    pub fn add_on_command(
        &mut self,
        name: String,
        handler_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptor {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get(&name) {
            Some(LaneLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_command handler for '{}'.", name),
            )),
            Some(LaneLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and value lane event handlers.",
                    name
                ),
            )),
            Some(LaneLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and map lane event handlers.",
                    name
                ),
            )),
            _ => {
                lane_lifecycles.insert(
                    name.clone(),
                    LaneLifecycle::Command(CommandLifecycleDescriptor::new(
                        name,
                        handler_type,
                        method,
                    )),
                );
                Ok(())
            }
        }
    }

    pub fn add_on_event(
        &mut self,
        name: String,
        handler_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptor {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(LaneLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and value lane event handlers.",
                    name
                ),
            )),
            Some(LaneLifecycle::Value(desc)) => desc.add_on_event(handler_type, method),
            Some(LaneLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both value and map lane event handlers.",
                    name
                ),
            )),
            _ => {
                lane_lifecycles.insert(
                    name.clone(),
                    LaneLifecycle::Value(ValueLifecycleDescriptor::new_on_event(
                        name,
                        handler_type,
                        method,
                    )),
                );
                Ok(())
            }
        }
    }

    pub fn add_on_set(
        &mut self,
        name: String,
        handler_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptor {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(LaneLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and value lane event handlers.",
                    name
                ),
            )),
            Some(LaneLifecycle::Value(desc)) => desc.add_on_set(handler_type, method),
            Some(LaneLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both value and map lane event handlers.",
                    name
                ),
            )),
            _ => {
                lane_lifecycles.insert(
                    name.clone(),
                    LaneLifecycle::Value(ValueLifecycleDescriptor::new_on_set(
                        name,
                        handler_type,
                        method,
                    )),
                );
                Ok(())
            }
        }
    }

    pub fn add_on_update(
        &mut self,
        name: String,
        map_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptor {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(LaneLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and map lane event handlers.",
                    name
                ),
            )),
            Some(LaneLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both value and map lane event handlers.",
                    name
                ),
            )),
            Some(LaneLifecycle::Map(desc)) => desc.add_on_update(map_type, method),
            _ => {
                lane_lifecycles.insert(
                    name.clone(),
                    LaneLifecycle::Map(MapLifecycleDescriptor::new_on_update(
                        name, map_type, method,
                    )),
                );
                Ok(())
            }
        }
    }

    pub fn add_on_remove(
        &mut self,
        name: String,
        map_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptor {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(LaneLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and map lane event handlers.",
                    name
                ),
            )),
            Some(LaneLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both value and map lane event handlers.",
                    name
                ),
            )),
            Some(LaneLifecycle::Map(desc)) => desc.add_on_remove(map_type, method),
            _ => {
                lane_lifecycles.insert(
                    name.clone(),
                    LaneLifecycle::Map(MapLifecycleDescriptor::new_on_remove(
                        name, map_type, method,
                    )),
                );
                Ok(())
            }
        }
    }

    pub fn add_on_clear(
        &mut self,
        name: String,
        map_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptor {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(LaneLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and map lane event handlers.",
                    name
                ),
            )),
            Some(LaneLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both value and map lane event handlers.",
                    name
                ),
            )),
            Some(LaneLifecycle::Map(desc)) => desc.add_on_clear(map_type, method),
            _ => {
                lane_lifecycles.insert(
                    name.clone(),
                    LaneLifecycle::Map(MapLifecycleDescriptor::new_on_clear(
                        name, map_type, method,
                    )),
                );
                Ok(())
            }
        }
    }
}

pub enum LaneLifecycle<'a> {
    Value(ValueLifecycleDescriptor<'a>),
    Command(CommandLifecycleDescriptor<'a>),
    Map(MapLifecycleDescriptor<'a>),
}

pub struct ValueLifecycleDescriptor<'a> {
    name: String,
    primary_lane_type: &'a Type,
    alternative_lane_types: HashSet<&'a Type>,
    on_event: Option<&'a Ident>,
    on_set: Option<&'a Ident>,
}

impl<'a> ValueLifecycleDescriptor<'a> {
    pub fn new_on_event(name: String, primary_lane_type: &'a Type, on_event: &'a Ident) -> Self {
        ValueLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types: Default::default(),
            on_event: Some(on_event),
            on_set: None,
        }
    }

    pub fn new_on_set(name: String, primary_lane_type: &'a Type, on_set: &'a Ident) -> Self {
        ValueLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types: Default::default(),
            on_event: None,
            on_set: Some(on_set),
        }
    }

    pub fn add_on_event(
        &mut self,
        lane_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let ValueLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types,
            on_event,
            ..
        } = self;
        if on_event.is_some() {
            Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_event handler for '{}'.", name),
            ))
        } else {
            if lane_type != *primary_lane_type {
                alternative_lane_types.insert(lane_type);
            }
            *on_event = Some(method);
            Ok(())
        }
    }

    pub fn add_on_set(&mut self, lane_type: &'a Type, method: &'a Ident) -> Result<(), syn::Error> {
        let ValueLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types,
            on_set,
            ..
        } = self;
        if on_set.is_some() {
            Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_set handler for '{}'.", name),
            ))
        } else {
            if lane_type != *primary_lane_type {
                alternative_lane_types.insert(lane_type);
            }
            *on_set = Some(method);
            Ok(())
        }
    }
}

pub struct CommandLifecycleDescriptor<'a> {
    pub name: String,
    pub primary_lane_type: &'a Type,
    pub on_command: &'a Ident,
}

impl<'a> CommandLifecycleDescriptor<'a> {
    pub fn new(name: String, primary_lane_type: &'a Type, on_command: &'a Ident) -> Self {
        CommandLifecycleDescriptor {
            name,
            primary_lane_type,
            on_command,
        }
    }
}

pub struct MapLifecycleDescriptor<'a> {
    name: String,
    primary_lane_type: &'a Type,
    alternative_lane_types: HashSet<&'a Type>,
    on_update: Option<&'a Ident>,
    on_remove: Option<&'a Ident>,
    on_clear: Option<&'a Ident>,
}

impl<'a> MapLifecycleDescriptor<'a> {
    pub fn new_on_update(name: String, map_type: &'a Type, on_update: &'a Ident) -> Self {
        MapLifecycleDescriptor {
            name,
            primary_lane_type: map_type,
            alternative_lane_types: Default::default(),
            on_update: Some(on_update),
            on_remove: None,
            on_clear: None,
        }
    }

    pub fn new_on_remove(name: String, map_type: &'a Type, on_remove: &'a Ident) -> Self {
        MapLifecycleDescriptor {
            name,
            primary_lane_type: map_type,
            alternative_lane_types: Default::default(),
            on_update: None,
            on_remove: Some(on_remove),
            on_clear: None,
        }
    }

    pub fn new_on_clear(name: String, map_type: &'a Type, on_clear: &'a Ident) -> Self {
        MapLifecycleDescriptor {
            name,
            primary_lane_type: map_type,
            alternative_lane_types: Default::default(),
            on_update: None,
            on_remove: None,
            on_clear: Some(on_clear),
        }
    }

    pub fn add_on_update(
        &mut self,
        map_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let MapLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types,
            on_update,
            ..
        } = self;
        if on_update.is_some() {
            Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_update handler for '{}'.", name),
            ))
        } else {
            if map_type != *primary_lane_type {
                alternative_lane_types.insert(map_type);
            }
            *on_update = Some(method);
            Ok(())
        }
    }

    pub fn add_on_remove(
        &mut self,
        map_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let MapLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types,
            on_remove,
            ..
        } = self;
        if on_remove.is_some() {
            Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_remove handler for '{}'.", name),
            ))
        } else {
            if map_type != *primary_lane_type {
                alternative_lane_types.insert(map_type);
            }
            *on_remove = Some(method);
            Ok(())
        }
    }

    pub fn add_on_clear(
        &mut self,
        map_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let MapLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types,
            on_clear,
            ..
        } = self;
        if on_clear.is_some() {
            Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_clear handler for '{}'.", name),
            ))
        } else {
            if map_type != *primary_lane_type {
                alternative_lane_types.insert(map_type);
            }
            *on_clear = Some(method);
            Ok(())
        }
    }
}
