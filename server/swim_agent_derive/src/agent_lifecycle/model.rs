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

use std::collections::{BTreeMap, HashSet};

use proc_macro2::Span;
use swim_utilities::errors::{
    validation::{Validation, ValidationItExt},
    Errors,
};
use syn::{
    parse_quote, AngleBracketedGenericArguments, Attribute, AttributeArgs, FnArg, GenericArgument,
    GenericParam, Ident, ImplItem, ImplItemMethod, Item, Lit, Meta, NestedMeta, Path,
    PathArguments, PathSegment, ReturnType, Signature, Type, TypePath, TypeReference,
};

use super::tree::BinTree;

const NOT_IMPL: &str = "The lifecycle annotation can only be applied to an impl block.";
const NO_GENERICS: &str = "Generic lifecycles are not yet supported.";
const INCONSISTENT_HANDLERS: &str = "Method marked with inconsistent handler attributes.";
const MANDATORY_SELF: &str = "The receiver of an event handler must be &self.";
const REQUIRED_CONTEXT: &str = "A HandlerContext parameter is required.";
const BAD_SIGNATURE: &str =
    "The handler does not have the correct signature for the annotated handler kind.";
const BAD_PARAMS: &str = "Invalid parameters to method handler annotation.";
const NO_AGENT: &str = "The name of the agent must be provided: e.g. #[lifecycle(MyAgent)].";
const EXTRA_PARAM: &str = "Unexpected attribute parameter.";
const BAD_PARAM: &str =
    "The parameter to the lifecycle attribute should be a path to an agent type.";
const ROOT: &str = "agent_root";
const NO_CLONE: &str = "no_clone";

/// Parameters passed to the lifecycle macro.
pub struct LifecycleArgs {
    agent_type: Path,        //Path to the agent type that the lifecycle is for.
    no_clone: bool,          //Whether the lifecycle type is not cloneable.
    root_path: Option<Path>, //Root path where the swim_agent crate is mounted in the module tree.
}

impl LifecycleArgs {
    fn new(agent_type: Path, no_clone: bool) -> Self {
        LifecycleArgs {
            agent_type,
            no_clone,
            root_path: None,
        }
    }

    fn with_root(agent_type: Path, no_clone: bool, root_path: Path) -> Self {
        LifecycleArgs {
            agent_type,
            no_clone,
            root_path: Some(root_path),
        }
    }
}

/// Validate the body of the 'lifecycle' attribute. This is require to have the form
/// `#[lifecycle(path::to::Agent)] where the path points to the struct type that defines
/// the lanes of an agent. This function will return the path if the body is of the
/// correct form.
///
/// #Arguments
/// * `item` - The item to which the attribute is attached (for error reporting).
/// * `args` - The attribute args.
pub fn validate_attr_args(
    item: &Item,
    args: AttributeArgs,
) -> Validation<LifecycleArgs, Errors<syn::Error>> {
    match args.as_slice() {
        [] => Validation::fail(syn::Error::new_spanned(item, NO_AGENT)),
        [NestedMeta::Meta(Meta::Path(agent))] => {
            Validation::valid(LifecycleArgs::new(agent.clone(), false))
        }
        [NestedMeta::Meta(Meta::Path(agent)), NestedMeta::Meta(Meta::Path(tag))]
            if tag.is_ident(NO_CLONE) =>
        {
            Validation::valid(LifecycleArgs::new(agent.clone(), true))
        }
        [NestedMeta::Meta(Meta::Path(agent)), second @ NestedMeta::Meta(Meta::List(lst))]
            if lst.path.is_ident(ROOT) =>
        {
            match lst.nested.first() {
                Some(NestedMeta::Meta(Meta::Path(root_path))) if lst.nested.len() == 1 => {
                    Validation::valid(LifecycleArgs::with_root(
                        agent.clone(),
                        false,
                        root_path.clone(),
                    ))
                }
                _ => Validation::fail(syn::Error::new_spanned(second, EXTRA_PARAM)),
            }
        }
        [NestedMeta::Meta(Meta::Path(agent)), NestedMeta::Meta(Meta::Path(tag)), third @ NestedMeta::Meta(Meta::List(lst))]
            if tag.is_ident(NO_CLONE) && lst.path.is_ident(ROOT) =>
        {
            match lst.nested.first() {
                Some(NestedMeta::Meta(Meta::Path(root_path))) if lst.nested.len() == 1 => {
                    Validation::valid(LifecycleArgs::with_root(
                        agent.clone(),
                        true,
                        root_path.clone(),
                    ))
                }
                _ => Validation::fail(syn::Error::new_spanned(third, EXTRA_PARAM)),
            }
        }
        [single] => Validation::fail(syn::Error::new_spanned(single, BAD_PARAM)),
        [_, second, ..] => Validation::fail(syn::Error::new_spanned(second, EXTRA_PARAM)),
    }
}

/// Remove the event handler annotations from all methods in an impl block.
pub fn strip_handler_attrs(
    item: &mut Item,
) -> Validation<Vec<Option<Vec<Attribute>>>, Errors<syn::Error>> {
    if let Item::Impl(block) = item {
        if !block.generics.params.is_empty() {
            return Validation::fail(syn::Error::new_spanned(block, NO_GENERICS));
        }
        let attrs = block
            .items
            .iter_mut()
            .map(|item| {
                if let ImplItem::Method(method) = item {
                    let (handler_attrs, others) =
                        method.attrs.drain(0..).partition::<Vec<_>, _>(assess_attr);
                    method.attrs.extend(others);
                    if handler_attrs.is_empty() {
                        None
                    } else {
                        Some(handler_attrs)
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        Validation::valid(attrs)
    } else {
        Validation::fail(syn::Error::new_spanned(item, NOT_IMPL))
    }
}

/// Validate an impl block as an agent lifecycle, returning a descriptor of all of the
/// lifecycle events (if they are valid). The `stripped_attrs` should be the output
/// of [`strip_handler_attrs`].
pub fn validate_with_attrs(
    lifecycle_args: LifecycleArgs,
    item: &Item,
    stripped_attrs: Vec<Option<Vec<Attribute>>>,
    default_route: Path,
) -> Validation<AgentLifecycleDescriptor<'_>, Errors<syn::Error>> {
    let LifecycleArgs {
        agent_type,
        no_clone,
        root_path,
    } = lifecycle_args;
    let root = root_path.unwrap_or(default_route);
    if let Item::Impl(block) = item {
        if !block.generics.params.is_empty() {
            return Validation::fail(syn::Error::new_spanned(block, NO_GENERICS));
        }
        let init = AgentLifecycleDescriptorBuilder::new(root, agent_type, no_clone, &block.self_ty);
        block
            .items
            .iter()
            .zip(stripped_attrs)
            .filter_map(|item_and_attrs| match item_and_attrs {
                (ImplItem::Method(method), Some(attrs)) => Some((method, attrs)),
                _ => None,
            })
            .validate_fold(Validation::valid(init), false, |acc, (method, attrs)| {
                validate_method(acc, attrs, method)
            })
    } else {
        Validation::fail(syn::Error::new_spanned(item, NOT_IMPL))
    }
    .map(AgentLifecycleDescriptorBuilder::build)
}

/// Descriptor for a single method, viewed as an event handler.
struct HandlerDescriptor {
    kind: HandlerKind,
    targets: HashSet<String>, // The lanes to which the handler should be attached.
}

impl HandlerDescriptor {
    fn new(kind: HandlerKind) -> Self {
        HandlerDescriptor {
            kind,
            targets: Default::default(),
        }
    }
}
/// Validate a single method and, if it is an event handler, attempt to add it to the
/// lifecycle descriptor.
fn validate_method<'a>(
    acc: AgentLifecycleDescriptorBuilder<'a>,
    attrs: Vec<Attribute>,
    method: &'a ImplItemMethod,
) -> Validation<AgentLifecycleDescriptorBuilder<'a>, Errors<syn::Error>> {
    let method_desc = attrs
        .iter()
        .append_fold(Validation::valid(None), true, |acc, attr| {
            match (acc, get_kind(attr)) {
                (Some(mut desc), Some(Ok((k, new_targets)))) => {
                    let HandlerDescriptor { kind, targets } = &mut desc;
                    if let Err(e) = kind.merge(method, k) {
                        Validation::Failed(Some(e))
                    } else {
                        targets.extend(new_targets);
                        Validation::valid(Some(desc))
                    }
                }
                (_, Some(Ok((kind, new_targets)))) => {
                    let mut desc = HandlerDescriptor::new(kind);
                    desc.targets.extend(new_targets);
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

/// Attempt to validate a method against an already defined descriptor.
fn validate_method_as<'a>(
    acc: AgentLifecycleDescriptorBuilder<'a>,
    descriptor: HandlerDescriptor,
    method: &'a ImplItemMethod,
) -> Validation<AgentLifecycleDescriptorBuilder<'a>, Errors<syn::Error>> {
    let acc = Validation::valid(acc);
    let HandlerDescriptor { kind, targets } = descriptor;
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
            HandlerKind::StartAndStop => {
                Validation::join(acc, validate_no_type_sig(sig)).and_then(|(mut acc, _)| {
                    let mut errors = Errors::empty();
                    if let Err(e) = acc.add_on_start(&sig.ident) {
                        errors.push(e);
                    }
                    if let Err(e) = acc.add_on_stop(&sig.ident) {
                        errors.push(e);
                    }
                    Validation::Validated(acc, errors)
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
            HandlerKind::Update => Validation::join(
                acc,
                validate_typed_sig(sig, 4, true).and_then(|t| hash_map_type_params(sig, t)),
            )
            .and_then(|(mut acc, (k, v))| {
                for target in targets {
                    if let Err(e) = acc.add_on_update(target, k, v, &sig.ident) {
                        return Validation::Validated(acc, Errors::of(e));
                    }
                }
                Validation::valid(acc)
            }),
            HandlerKind::Remove => Validation::join(
                acc,
                validate_typed_sig(sig, 3, true).and_then(|t| hash_map_type_params(sig, t)),
            )
            .and_then(|(mut acc, (k, v))| {
                for target in targets {
                    if let Err(e) = acc.add_on_remove(target, k, v, &sig.ident) {
                        return Validation::Validated(acc, Errors::of(e));
                    }
                }
                Validation::valid(acc)
            }),
            HandlerKind::Clear => Validation::join(
                acc,
                validate_typed_sig(sig, 1, false).and_then(|t| hash_map_type_params(sig, t)),
            )
            .and_then(|(mut acc, (k, v))| {
                for target in targets {
                    if let Err(e) = acc.add_on_clear(target, k, v, &sig.ident) {
                        return Validation::Validated(acc, Errors::of(e));
                    }
                }
                Validation::valid(acc)
            }),
            HandlerKind::JoinValue => Validation::join(acc, validate_join_lifecycle_sig(sig))
                .and_then(|(mut acc, (k, v))| {
                    for target in targets {
                        if let Err(e) = acc.add_join_lifecycle(target, k, v, &sig.ident) {
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
const DUPLICATE_ON_STOP: &str = "Duplicate on_stop event handler.";
const DUPLICATE_ON_START: &str = "Duplicate on_start event handler.";

/// Check common properties that all event handler signatures should have.
fn check_sig_common(sig: &Signature) -> Result<(), syn::Error> {
    if sig.asyncness.is_some() {
        Err(NO_ASYNC)
    } else if sig.unsafety.is_some() {
        Err(NO_UNSAFE)
    } else if matches!(sig.output, ReturnType::Default) {
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

/// Check that a method has the correct shape for the on_start or on_stop handlers.
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
            if param_types.is_empty() {
                Validation::valid(())
            } else {
                Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE))
            }
        })
}

fn validate_join_lifecycle_sig(sig: &Signature) -> Validation<(&Type, &Type), Errors<syn::Error>> {
    let iter = sig.inputs.iter();
    check_receiver(sig, iter).and_then(|iter| {
        let param_types = extract_types(iter);
        match param_types.first() {
            Some(context_type) if param_types.len() == 1 => {
                extract_join_value_params(sig, context_type)
            }
            _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
        }
    })
}

const JOIN_VALUE_CONTEXT: &str = "JoinValueContext";

fn extract_join_value_params<'a>(
    sig: &Signature,
    param_type: &'a Type,
) -> Validation<(&'a Type, &'a Type), Errors<syn::Error>> {
    match param_type {
        Type::Path(TypePath { qself: None, path }) => match path.segments.last() {
            Some(PathSegment {
                ident,
                arguments:
                    PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }),
            }) if ident == JOIN_VALUE_CONTEXT && args.len() == 3 => {
                match (&args[0], &args[1], &args[2]) {
                    (
                        GenericArgument::Type(_),
                        GenericArgument::Type(key_type),
                        GenericArgument::Type(value_type),
                    ) => Validation::valid((key_type, value_type)),
                    _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
                }
            }
            _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
        },
        _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
    }
}

/// Check a method for use as a lane lifecycle handler. Returns the type that the lane should
/// have.
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
            match param_types.first() {
                Some(rep_type) if param_types.len() == expected_params => {
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
                _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
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

const HASH_MAP: &str = "HashMap";

fn hash_map_type_params<'a>(
    sig: &Signature,
    map_type: &'a Type,
) -> Validation<(&'a Type, &'a Type), Errors<syn::Error>> {
    match map_type {
        Type::Path(TypePath { qself: None, path }) => match path.segments.last() {
            Some(PathSegment {
                ident,
                arguments:
                    PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }),
            }) if ident == HASH_MAP && (args.len() == 2 || args.len() == 3) => {
                match (&args[0], &args[1]) {
                    (GenericArgument::Type(key_type), GenericArgument::Type(value_type)) => {
                        Validation::valid((key_type, value_type))
                    }
                    _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
                }
            }
            _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
        },
        _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
    }
}

/// Check that the receiver of the method is &self.
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

/// Try to process an attribute to get the kind of the handler.
fn get_kind(attr: &Attribute) -> Option<Result<(HandlerKind, Vec<String>), syn::Error>> {
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
            JOIN_VALUE => Some(HandlerKind::JoinValue),
            _ => None,
        };
        kind.map(|k| match &k {
            HandlerKind::Start | HandlerKind::Stop => {
                if attr.tokens.is_empty() {
                    Ok((k, vec![]))
                } else {
                    Err(syn::Error::new_spanned(attr, BAD_PARAMS))
                }
            }
            _ => extract_targets(attr).map(move |targets| (k, targets)),
        })
    } else {
        None
    }
}

/// Extract the lanes to which a handler should be attached. This supports a comma
/// separated list of literal strings or identifiers.
fn extract_targets(attr: &Attribute) -> Result<Vec<String>, syn::Error> {
    let meta = attr.parse_meta()?;
    let bad_params = || syn::Error::new_spanned(attr, BAD_PARAMS);
    match meta {
        Meta::List(lst) => lst
            .nested
            .iter()
            .try_fold(vec![], |mut acc, nested| match nested {
                NestedMeta::Meta(Meta::Path(Path {
                    leading_colon: None,
                    segments,
                })) => match segments.first() {
                    Some(PathSegment {
                        ident,
                        arguments: PathArguments::None,
                    }) if segments.len() == 1 => {
                        acc.push(ident.to_string());
                        Ok(acc)
                    }
                    _ => Err(bad_params()),
                },
                NestedMeta::Lit(Lit::Str(name)) if lst.nested.len() == 1 => {
                    acc.push(name.value());
                    Ok(acc)
                }
                _ => Err(bad_params()),
            })
            .and_then(|targets| {
                if targets.is_empty() {
                    Err(bad_params())
                } else {
                    Ok(targets)
                }
            }),
        _ => Err(bad_params()),
    }
}

/// The different kinds of handler that can occur in a lifecycle.
#[derive(PartialEq, Eq)]
enum HandlerKind {
    Start,
    Stop,
    StartAndStop, // Indicates that a single method is used for the on_start and on_stop events.
    Command,
    Event,
    Set,
    Update,
    Remove,
    Clear,
    JoinValue,
}

impl HandlerKind {
    fn merge(&mut self, sig: &ImplItemMethod, other: HandlerKind) -> Result<(), syn::Error> {
        match (self, other) {
            (k @ HandlerKind::Start, HandlerKind::Stop)
            | (k @ HandlerKind::Stop, HandlerKind::Start) => {
                *k = HandlerKind::StartAndStop;
                Ok(())
            }
            (HandlerKind::StartAndStop, HandlerKind::Start | HandlerKind::Stop) => Ok(()),
            (k1, k2) => {
                if *k1 == k2 {
                    Ok(())
                } else {
                    Err(syn::Error::new_spanned(sig, INCONSISTENT_HANDLERS))
                }
            }
        }
    }
}

const ON_START: &str = "on_start";
const ON_STOP: &str = "on_stop";
const ON_COMMAND: &str = "on_command";
const ON_EVENT: &str = "on_event";
const ON_SET: &str = "on_set";
const ON_UPDATE: &str = "on_update";
const ON_REMOVE: &str = "on_remove";
const ON_CLEAR: &str = "on_clear";
const JOIN_VALUE: &str = "join_value_lifecycle";

/// Check if an attribute is an event handler attribute. This simple checks the name and not
/// the contents.
fn assess_attr(attr: &Attribute) -> bool {
    let path = &attr.path;
    if path.leading_colon.is_some() {
        false
    } else {
        match path.segments.first() {
            Some(seg) if path.segments.len() == 1 && seg.arguments.is_empty() => {
                let seg_str = seg.ident.to_string();
                matches!(
                    seg_str.as_str(),
                    ON_START
                        | ON_STOP
                        | ON_COMMAND
                        | ON_EVENT
                        | ON_SET
                        | ON_UPDATE
                        | ON_REMOVE
                        | ON_CLEAR
                        | JOIN_VALUE
                )
            }
            _ => false,
        }
    }
}

pub struct JoinValueInit<'a> {
    pub name: String,
    pub lifecycle: &'a Ident,
}

impl<'a> JoinValueInit<'a> {
    pub fn new(name: String, lifecycle: &'a Ident) -> Self {
        JoinValueInit { name, lifecycle }
    }

    pub fn item_ident(&self) -> Ident {
        Ident::new(&self.name, Span::call_site())
    }
}

/// Descriptor of an agent lifecycle, extracted from an impl block.
pub struct AgentLifecycleDescriptor<'a> {
    pub root: Path,               //The root module path.
    pub agent_type: Path,         //The agent this is a lifecycle of.
    pub no_clone: bool,           //Whether the lifecycle is not cloneable.
    pub lifecycle_type: &'a Type, //The type of the lifecycle (taken from the impl block).
    pub init_blocks: Vec<JoinValueInit<'a>>,
    pub on_start: Option<&'a Ident>, //A handler attached to the on_start event.
    pub on_stop: Option<&'a Ident>,  //A handler attached to the on_stop event.
    pub lane_lifecycles: BinTree<String, ItemLifecycle<'a>>, //Labelled tree of lane handlers.
}

/// Builder type for constructing an [`AgentLifecycleDescriptor`].
pub struct AgentLifecycleDescriptorBuilder<'a> {
    pub root: Path,
    pub agent_type: Path,
    pub no_clone: bool,
    pub lifecycle_type: &'a Type,
    pub on_start: Option<&'a Ident>,
    pub on_stop: Option<&'a Ident>,
    pub lane_lifecycles: BTreeMap<String, ItemLifecycle<'a>>,
}

impl<'a> AgentLifecycleDescriptorBuilder<'a> {
    pub fn new(root: Path, agent_type: Path, no_clone: bool, lifecycle_type: &'a Type) -> Self {
        AgentLifecycleDescriptorBuilder {
            root,
            agent_type,
            no_clone,
            lifecycle_type,
            on_start: None,
            on_stop: None,
            lane_lifecycles: BTreeMap::new(),
        }
    }

    pub fn build(self) -> AgentLifecycleDescriptor<'a> {
        let AgentLifecycleDescriptorBuilder {
            root,
            agent_type,
            no_clone,
            lifecycle_type,
            on_start,
            on_stop,
            lane_lifecycles,
        } = self;

        let init_blocks = lane_lifecycles
            .values()
            .filter_map(|item| match item {
                ItemLifecycle::Map(MapLifecycleDescriptor {
                    name,
                    join_lifecycle: Some(join_lc),
                    ..
                }) => Some(JoinValueInit::new(name.clone(), join_lc)),
                _ => None,
            })
            .collect();

        AgentLifecycleDescriptor {
            root,
            agent_type,
            no_clone,
            lifecycle_type,
            init_blocks,
            on_start,
            on_stop,
            lane_lifecycles: BinTree::from(lane_lifecycles),
        }
    }

    pub fn add_on_stop(&mut self, method: &'a Ident) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder { on_stop, .. } = self;
        if on_stop.is_some() {
            Err(syn::Error::new_spanned(method, DUPLICATE_ON_STOP))
        } else {
            *on_stop = Some(method);
            Ok(())
        }
    }

    pub fn add_on_start(&mut self, method: &'a Ident) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder { on_start, .. } = self;
        if on_start.is_some() {
            Err(syn::Error::new_spanned(method, DUPLICATE_ON_START))
        } else {
            *on_start = Some(method);
            Ok(())
        }
    }

    pub fn add_join_lifecycle(
        &mut self,
        name: String,
        key_type: &'a Type,
        value_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(ItemLifecycle::Map(desc)) => desc.add_join_lifecycle(key_type, value_type, method),
            None => {
                lane_lifecycles.insert(
                    name.clone(),
                    ItemLifecycle::Map(MapLifecycleDescriptor::new_join_lifecycle(
                        name,
                        (key_type, value_type),
                        method,
                    )),
                );
                Ok(())
            }
            _ => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both a join lane lifecycle and non-map event handlers.",
                    name
                ),
            )),
        }
    }

    pub fn add_on_command(
        &mut self,
        name: String,
        handler_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get(&name) {
            Some(ItemLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_command handler for '{}'.", name),
            )),
            Some(ItemLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and value lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and map lane event handlers.",
                    name
                ),
            )),
            _ => {
                lane_lifecycles.insert(
                    name.clone(),
                    ItemLifecycle::Command(CommandLifecycleDescriptor::new(
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
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(ItemLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and value lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Value(desc)) => desc.add_on_event(handler_type, method),
            Some(ItemLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both value and map lane event handlers.",
                    name
                ),
            )),
            _ => {
                lane_lifecycles.insert(
                    name.clone(),
                    ItemLifecycle::Value(ValueLifecycleDescriptor::new_on_event(
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
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(ItemLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and value lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Value(desc)) => desc.add_on_set(handler_type, method),
            Some(ItemLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both value and map lane event handlers.",
                    name
                ),
            )),
            _ => {
                lane_lifecycles.insert(
                    name.clone(),
                    ItemLifecycle::Value(ValueLifecycleDescriptor::new_on_set(
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
        key_type: &'a Type,
        value_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(ItemLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both value and map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Map(desc)) => desc.add_on_update(key_type, value_type, method),
            _ => {
                let map_type = (key_type, value_type);
                lane_lifecycles.insert(
                    name.clone(),
                    ItemLifecycle::Map(MapLifecycleDescriptor::new_on_update(
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
        key_type: &'a Type,
        value_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(ItemLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both value and map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Map(desc)) => desc.add_on_remove(key_type, value_type, method),
            _ => {
                let map_type = (key_type, value_type);
                lane_lifecycles.insert(
                    name.clone(),
                    ItemLifecycle::Map(MapLifecycleDescriptor::new_on_remove(
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
        key_type: &'a Type,
        value_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(ItemLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both value and map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Map(desc)) => desc.add_on_clear(key_type, value_type, method),
            _ => {
                let map_type = (key_type, value_type);
                lane_lifecycles.insert(
                    name.clone(),
                    ItemLifecycle::Map(MapLifecycleDescriptor::new_on_clear(
                        name, map_type, method,
                    )),
                );
                Ok(())
            }
        }
    }
}

/// Lifecycle attached to a single item.
pub enum ItemLifecycle<'a> {
    Value(ValueLifecycleDescriptor<'a>),
    Command(CommandLifecycleDescriptor<'a>),
    Map(MapLifecycleDescriptor<'a>),
}

impl<'a> ItemLifecycle<'a> {
    fn item_name(&self) -> &str {
        let name = match self {
            ItemLifecycle::Value(ValueLifecycleDescriptor { name, .. }) => name,
            ItemLifecycle::Command(CommandLifecycleDescriptor { name, .. }) => name,
            ItemLifecycle::Map(MapLifecycleDescriptor { name, .. }) => name,
        };
        name.as_str()
    }

    pub fn item_ident(&self) -> Ident {
        let name = self.item_name();
        Ident::new(name, Span::call_site())
    }

    /// The type of node to create for the heterogeneous tree of item lifecycles in
    /// the agent lifecycle.
    pub fn branch_type(&self, root: &syn::Path) -> Path {
        match self {
            ItemLifecycle::Value(ValueLifecycleDescriptor { .. }) => {
                parse_quote! {
                    #root::agent_lifecycle::item_event::ValueLikeBranch
                }
            }
            ItemLifecycle::Command(CommandLifecycleDescriptor { .. }) => {
                parse_quote! {
                    #root::agent_lifecycle::item_event::CommandBranch
                }
            }
            ItemLifecycle::Map(MapLifecycleDescriptor { .. }) => {
                parse_quote! {
                    #root::agent_lifecycle::item_event::MapLikeBranch
                }
            }
        }
    }
}

pub struct ValueLifecycleDescriptor<'a> {
    name: String,                              //The name of the lane.
    primary_lane_type: &'a Type,               //First observed type of the lane.
    alternative_lane_types: HashSet<&'a Type>, //Further types observed for the lane.
    pub on_event: Option<&'a Ident>,
    pub on_set: Option<&'a Ident>,
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
    pub name: String, //The nam eo the lane.
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
    name: String,                                          //The name of the lane.
    primary_lane_type: (&'a Type, &'a Type),               //First observed type of the lane.
    alternative_lane_types: HashSet<(&'a Type, &'a Type)>, //Further types observed for the lane.
    pub on_update: Option<&'a Ident>,
    pub on_remove: Option<&'a Ident>,
    pub on_clear: Option<&'a Ident>,
    pub join_lifecycle: Option<&'a Ident>,
}

impl<'a> MapLifecycleDescriptor<'a> {
    pub fn new_on_update(
        name: String,
        map_type: (&'a Type, &'a Type),
        on_update: &'a Ident,
    ) -> Self {
        MapLifecycleDescriptor {
            name,
            primary_lane_type: map_type,
            alternative_lane_types: Default::default(),
            on_update: Some(on_update),
            on_remove: None,
            on_clear: None,
            join_lifecycle: None,
        }
    }

    pub fn new_on_remove(
        name: String,
        map_type: (&'a Type, &'a Type),
        on_remove: &'a Ident,
    ) -> Self {
        MapLifecycleDescriptor {
            name,
            primary_lane_type: map_type,
            alternative_lane_types: Default::default(),
            on_update: None,
            on_remove: Some(on_remove),
            on_clear: None,
            join_lifecycle: None,
        }
    }

    pub fn new_on_clear(name: String, map_type: (&'a Type, &'a Type), on_clear: &'a Ident) -> Self {
        MapLifecycleDescriptor {
            name,
            primary_lane_type: map_type,
            alternative_lane_types: Default::default(),
            on_update: None,
            on_remove: None,
            on_clear: Some(on_clear),
            join_lifecycle: None,
        }
    }

    pub fn new_join_lifecycle(
        name: String,
        map_type: (&'a Type, &'a Type),
        join_lifecycle: &'a Ident,
    ) -> Self {
        MapLifecycleDescriptor {
            name,
            primary_lane_type: map_type,
            alternative_lane_types: Default::default(),
            on_update: None,
            on_remove: None,
            on_clear: None,
            join_lifecycle: Some(join_lifecycle),
        }
    }

    pub fn add_on_update(
        &mut self,
        key_type: &'a Type,
        value_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let MapLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types,
            on_update,
            ..
        } = self;
        let map_type = (key_type, value_type);
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
        key_type: &'a Type,
        value_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let MapLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types,
            on_remove,
            ..
        } = self;
        let map_type = (key_type, value_type);
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
        key_type: &'a Type,
        value_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let MapLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types,
            on_clear,
            ..
        } = self;
        let map_type = (key_type, value_type);
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

    pub fn add_join_lifecycle(
        &mut self,
        key_type: &'a Type,
        value_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let MapLifecycleDescriptor {
            name,
            primary_lane_type,
            alternative_lane_types,
            join_lifecycle,
            ..
        } = self;
        let map_type = (key_type, value_type);
        if join_lifecycle.is_some() {
            Err(syn::Error::new_spanned(
                method,
                format!("Duplicate join lane lifecycle for '{}'.", name),
            ))
        } else {
            *join_lifecycle = Some(method);
            if map_type != *primary_lane_type {
                alternative_lane_types.insert(map_type);
            }
            Ok(())
        }
    }
}
