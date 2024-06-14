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

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
};

use proc_macro2::Span;
use swimos_utilities::errors::{Errors, Validation, ValidationItExt};
use syn::{
    parse_quote, AngleBracketedGenericArguments, Attribute, AttributeArgs, Binding, FnArg,
    GenericArgument, GenericParam, Ident, ImplItem, ImplItemMethod, Item, Lit, Meta, NestedMeta,
    Path, PathArguments, PathSegment, ReturnType, Signature, TraitBound, Type, TypeImplTrait,
    TypeParamBound, TypePath, TypeReference,
};

use super::tree::BinTree;

const NOT_IMPL: &str = "The lifecycle annotation can only be applied to an impl block.";
const NO_GENERICS: &str = "Generic lifecycles are not yet supported.";
const INCONSISTENT_HANDLERS: &str = "Method marked with inconsistent handler attributes.";
const MANDATORY_SELF: &str = "The receiver of an event handler must be &self.";
const REQUIRED_CONTEXT: &str = "A HandlerContext parameter is required.";
const REQUIRED_HTTP_CONTEXT: &str = "An HttpContext parameter is required.";
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
    root_path: Option<Path>, //Root path where the swimos_agent crate is mounted in the module tree.
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
/// # Arguments
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
            HandlerKind::Cue => {
                Validation::join(acc, validate_cue_sig(sig)).and_then(|(mut acc, t)| {
                    for target in targets {
                        if let Err(e) = acc.add_on_cue(target, t, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                })
            }
            HandlerKind::Keys => {
                Validation::join(acc, validate_keys_sig(sig)).and_then(|(mut acc, k)| {
                    for target in targets {
                        if let Err(e) = acc.add_demand_map_keys(target, k, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                })
            }
            HandlerKind::CueKey => {
                Validation::join(acc, validate_cue_key_sig(sig)).and_then(|(mut acc, (k, v))| {
                    for target in targets {
                        if let Err(e) = acc.add_on_cue_key(target, k, v, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                })
            }
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
            HandlerKind::JoinMap => Validation::join(acc, validate_join_map_lifecycle_sig(sig))
                .and_then(|(mut acc, (l, k, v))| {
                    for target in targets {
                        if let Err(e) = acc.add_join_map_lifecycle(target, l, k, v, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                }),
            HandlerKind::JoinValue => Validation::join(acc, validate_join_value_lifecycle_sig(sig))
                .and_then(|(mut acc, (k, v))| {
                    for target in targets {
                        if let Err(e) = acc.add_join_value_lifecycle(target, k, v, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                }),
            HandlerKind::Get => {
                Validation::join(acc, validate_get_or_delete_sig(sig)).and_then(|(mut acc, t)| {
                    for target in targets {
                        if let Err(e) = acc.add_on_get(target, t.clone(), &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                })
            }
            HandlerKind::Post => {
                Validation::join(acc, validate_post_or_put_sig(sig)).and_then(|(mut acc, t)| {
                    for target in targets {
                        if let Err(e) = acc.add_on_post(target, t, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                })
            }
            HandlerKind::Put => {
                Validation::join(acc, validate_post_or_put_sig(sig)).and_then(|(mut acc, t)| {
                    for target in targets {
                        if let Err(e) = acc.add_on_put(target, t, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                })
            }
            HandlerKind::Delete => {
                Validation::join(acc, validate_get_or_delete_sig(sig)).and_then(|(mut acc, _)| {
                    for target in targets {
                        if let Err(e) = acc.add_on_delete(target, &sig.ident) {
                            return Validation::Validated(acc, Errors::of(e));
                        }
                    }
                    Validation::valid(acc)
                })
            }
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

fn validate_join_value_lifecycle_sig(
    sig: &Signature,
) -> Validation<(&Type, &Type), Errors<syn::Error>> {
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

fn validate_join_map_lifecycle_sig(
    sig: &Signature,
) -> Validation<(&Type, &Type, &Type), Errors<syn::Error>> {
    let iter = sig.inputs.iter();
    check_receiver(sig, iter).and_then(|iter| {
        let param_types = extract_types(iter);
        match param_types.first() {
            Some(context_type) if param_types.len() == 1 => {
                extract_join_map_params(sig, context_type)
            }
            _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
        }
    })
}

const JOIN_VALUE_CONTEXT: &str = "JoinValueContext";
const JOIN_MAP_CONTEXT: &str = "JoinMapContext";

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

fn extract_join_map_params<'a>(
    sig: &Signature,
    param_type: &'a Type,
) -> Validation<(&'a Type, &'a Type, &'a Type), Errors<syn::Error>> {
    match param_type {
        Type::Path(TypePath { qself: None, path }) => match path.segments.last() {
            Some(PathSegment {
                ident,
                arguments:
                    PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }),
            }) if ident == JOIN_MAP_CONTEXT && args.len() == 4 => {
                match (&args[0], &args[1], &args[2], &args[3]) {
                    (
                        GenericArgument::Type(_),
                        GenericArgument::Type(link_key_type),
                        GenericArgument::Type(key_type),
                        GenericArgument::Type(value_type),
                    ) => Validation::valid((link_key_type, key_type, value_type)),
                    _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
                }
            }
            _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
        },
        _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
    }
}

fn validate_cue_sig(sig: &Signature) -> Validation<Option<&Type>, Errors<syn::Error>> {
    let iter = sig.inputs.iter();
    let inputs = check_receiver(sig, iter)
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
        });
    let output = match &sig.output {
        ReturnType::Default => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
        ReturnType::Type(_, t) => extract_handler_ret(sig, t.as_ref()),
    };
    inputs.join(output).map(|(_, t)| t)
}

fn validate_keys_sig(sig: &Signature) -> Validation<&Type, Errors<syn::Error>> {
    let iter = sig.inputs.iter();
    let inputs = check_receiver(sig, iter)
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
        });
    let output = match &sig.output {
        ReturnType::Default => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
        ReturnType::Type(_, t) => extract_handler_ret(sig, t).and_then(|maybe_ret| {
            if let Some(ret) = maybe_ret {
                extract_hash_set_type_param(sig, ret)
            } else {
                Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE))
            }
        }),
    };
    inputs.join(output).map(|(_, t)| t)
}

fn validate_cue_key_sig(sig: &Signature) -> Validation<(&Type, &Type), Errors<syn::Error>> {
    let iter = sig.inputs.iter();
    let inputs = check_receiver(sig, iter)
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
                Some(key_type) if param_types.len() == 1 => Validation::valid(*key_type),
                _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
            }
        });
    let output = match &sig.output {
        ReturnType::Default => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
        ReturnType::Type(_, t) => extract_handler_ret(sig, t.as_ref()).and_then(|maybe_val| {
            if let Some(t) = maybe_val {
                extract_option_type_param(sig, t)
            } else {
                Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE))
            }
        }),
    };
    inputs.join(output)
}

const HANDLER_ACTION_NAME: &str = "HandlerAction";
const EVENT_HANDLER_NAME: &str = "EventHandler";
const COMPLETION_ASSOC_NAME: &str = "Completion";

fn extract_handler_ret<'a>(
    sig: &'a Signature,
    ret_type: &'a Type,
) -> Validation<Option<&'a Type>, Errors<syn::Error>> {
    match ret_type {
        Type::ImplTrait(TypeImplTrait { bounds, .. }) => {
            if let Some(params) = bounds.iter().find_map(|bound| match bound {
                TypeParamBound::Trait(TraitBound { path, .. }) => {
                    if let Some(PathSegment { ident, arguments }) = path.segments.last() {
                        if ident == HANDLER_ACTION_NAME || ident == EVENT_HANDLER_NAME {
                            Some(arguments)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            }) {
                match params {
                    PathArguments::AngleBracketed(AngleBracketedGenericArguments {
                        args, ..
                    }) => {
                        let completion_type = args.iter().find_map(|arg| match arg {
                            GenericArgument::Binding(Binding { ident, ty, .. })
                                if ident == COMPLETION_ASSOC_NAME =>
                            {
                                Some(ty)
                            }
                            _ => None,
                        });
                        Validation::valid(completion_type)
                    }
                    _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
                }
            } else {
                Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE))
            }
        }
        _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
    }
}

fn validate_get_or_delete_sig(sig: &Signature) -> Validation<Cow<'_, Type>, Errors<syn::Error>> {
    let iter = sig.inputs.iter();
    let inputs = check_receiver(sig, iter)
        .and_then(|mut iter| {
            if iter.next().is_none() {
                Validation::fail(syn::Error::new_spanned(sig, REQUIRED_CONTEXT))
            } else if iter.next().is_none() {
                Validation::fail(syn::Error::new_spanned(sig, REQUIRED_HTTP_CONTEXT))
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
        });
    let output = match &sig.output {
        ReturnType::Default => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
        ReturnType::Type(_, t) => extract_handler_ret(sig, t.as_ref()),
    }
    .and_then(|maybe_t| {
        if let Some(t) = maybe_t {
            if is_unit_response(t) {
                Validation::valid(Cow::Owned(parse_quote!(())))
            } else {
                extract_single_type_param(sig, t, RESPONSE, false).map(Cow::Borrowed)
            }
        } else {
            Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE))
        }
    });
    inputs.join(output).map(|(_, t)| t)
}

fn is_unit_response(t: &Type) -> bool {
    match t {
        Type::Path(TypePath { qself: None, path }) => match path.segments.last() {
            Some(PathSegment { ident, arguments }) if arguments.is_empty() => {
                ident == UNIT_RESPONSE
            }
            _ => false,
        },
        _ => false,
    }
}

fn validate_post_or_put_sig(sig: &Signature) -> Validation<&Type, Errors<syn::Error>> {
    let iter = sig.inputs.iter();
    let inputs = check_receiver(sig, iter)
        .and_then(|mut iter| {
            if iter.next().is_none() {
                Validation::fail(syn::Error::new_spanned(sig, REQUIRED_CONTEXT))
            } else if iter.next().is_none() {
                Validation::fail(syn::Error::new_spanned(sig, REQUIRED_HTTP_CONTEXT))
            } else {
                Validation::valid(iter)
            }
        })
        .and_then(|iter| {
            let param_types = extract_types(iter);
            match param_types.as_slice() {
                [value_type] => Validation::valid(*value_type),
                _ => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
            }
        });
    let output = match &sig.output {
        ReturnType::Default => Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE)),
        ReturnType::Type(_, t) => extract_handler_ret(sig, t.as_ref()),
    }
    .and_then(|maybe_t| {
        if let Some(t) = maybe_t {
            if is_unit_response(t) {
                Validation::valid(Cow::Owned(parse_quote!(())))
            } else {
                extract_single_type_param(sig, t, RESPONSE, false).map(Cow::Borrowed)
            }
        } else {
            Validation::fail(syn::Error::new_spanned(sig, BAD_SIGNATURE))
        }
    });
    inputs.join(output).map(|(t, _)| t)
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
const OPTION: &str = "Option";
const HASH_SET: &str = "HashSet";
const RESPONSE: &str = "Response";
const UNIT_RESPONSE: &str = "UnitResponse";

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

fn extract_option_type_param<'a>(
    sig: &Signature,
    parameterized: &'a Type,
) -> Validation<&'a Type, Errors<syn::Error>> {
    extract_single_type_param(sig, parameterized, OPTION, false)
}

fn extract_hash_set_type_param<'a>(
    sig: &Signature,
    parameterized: &'a Type,
) -> Validation<&'a Type, Errors<syn::Error>> {
    extract_single_type_param(sig, parameterized, HASH_SET, true)
}

fn extract_single_type_param<'a>(
    sig: &Signature,
    parameterized: &'a Type,
    expected_name: &str,
    allow_extra: bool,
) -> Validation<&'a Type, Errors<syn::Error>> {
    match parameterized {
        Type::Path(TypePath { qself: None, path }) => match path.segments.last() {
            Some(PathSegment {
                ident,
                arguments:
                    PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }),
            }) if ident == expected_name
                && (args.len() == 1 || (allow_extra && args.len() == 2)) =>
            {
                match &args[0] {
                    GenericArgument::Type(param_type) => Validation::valid(param_type),
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
            ON_CUE => Some(HandlerKind::Cue),
            KEYS => Some(HandlerKind::Keys),
            ON_CUE_KEY => Some(HandlerKind::CueKey),
            ON_EVENT => Some(HandlerKind::Event),
            ON_SET => Some(HandlerKind::Set),
            ON_UPDATE => Some(HandlerKind::Update),
            ON_REMOVE => Some(HandlerKind::Remove),
            ON_CLEAR => Some(HandlerKind::Clear),
            JOIN_MAP => Some(HandlerKind::JoinMap),
            JOIN_VALUE => Some(HandlerKind::JoinValue),
            ON_GET => Some(HandlerKind::Get),
            ON_POST => Some(HandlerKind::Post),
            ON_PUT => Some(HandlerKind::Put),
            ON_DELETE => Some(HandlerKind::Delete),
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
    Cue,
    Keys,
    CueKey,
    Event,
    Set,
    Update,
    Remove,
    Clear,
    JoinMap,
    JoinValue,
    Get,
    Post,
    Put,
    Delete,
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
const ON_CUE: &str = "on_cue";
const KEYS: &str = "keys";
const ON_CUE_KEY: &str = "on_cue_key";
const ON_EVENT: &str = "on_event";
const ON_SET: &str = "on_set";
const ON_UPDATE: &str = "on_update";
const ON_REMOVE: &str = "on_remove";
const ON_CLEAR: &str = "on_clear";
const JOIN_VALUE: &str = "join_value_lifecycle";
const JOIN_MAP: &str = "join_map_lifecycle";
const ON_GET: &str = "on_get";
const ON_POST: &str = "on_post";
const ON_PUT: &str = "on_put";
const ON_DELETE: &str = "on_delete";

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
                        | ON_CUE
                        | KEYS
                        | ON_CUE_KEY
                        | ON_EVENT
                        | ON_SET
                        | ON_UPDATE
                        | ON_REMOVE
                        | ON_CLEAR
                        | JOIN_MAP
                        | JOIN_VALUE
                        | ON_GET
                        | ON_POST
                        | ON_PUT
                        | ON_DELETE
                )
            }
            _ => false,
        }
    }
}

#[derive(Clone, Copy)]
pub enum JoinLaneKind {
    Map,
    Value,
}

pub struct JoinLaneInit<'a> {
    pub name: String,
    pub lifecycle: &'a Ident,
    pub kind: JoinLaneKind,
}

impl<'a> JoinLaneInit<'a> {
    pub fn new(name: String, kind: JoinLaneKind, lifecycle: &'a Ident) -> Self {
        JoinLaneInit {
            name,
            kind,
            lifecycle,
        }
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
    pub init_blocks: Vec<JoinLaneInit<'a>>,
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
                    join_lifecycle: JoinLifecycle::JoinValue(join_lc),
                    ..
                }) => Some(JoinLaneInit::new(
                    name.clone(),
                    JoinLaneKind::Value,
                    join_lc,
                )),
                ItemLifecycle::Map(MapLifecycleDescriptor {
                    name,
                    join_lifecycle: JoinLifecycle::JoinMap(_, join_lc),
                    ..
                }) => Some(JoinLaneInit::new(name.clone(), JoinLaneKind::Map, join_lc)),
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

    pub fn add_join_map_lifecycle(
        &mut self,
        name: String,
        link_key_type: &'a Type,
        key_type: &'a Type,
        value_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(ItemLifecycle::Map(desc)) => {
                desc.add_join_map_lifecycle(link_key_type, key_type, value_type, method)
            }
            None => {
                lane_lifecycles.insert(
                    name.clone(),
                    ItemLifecycle::Map(MapLifecycleDescriptor::new_join_map_lifecycle(
                        name,
                        link_key_type,
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

    pub fn add_join_value_lifecycle(
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
            Some(ItemLifecycle::Map(desc)) => {
                desc.add_join_value_lifecycle(key_type, value_type, method)
            }
            None => {
                lane_lifecycles.insert(
                    name.clone(),
                    ItemLifecycle::Map(MapLifecycleDescriptor::new_join_value_lifecycle(
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
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and demand lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::DemandMap(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and demand-map lane event handlers.",
                    name
                ),
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
            Some(ItemLifecycle::Http(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both command and HTTP lane event handlers.",
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

    pub fn add_on_cue(
        &mut self,
        name: String,
        handler_type: Option<&'a Type>,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get(&name) {
            Some(ItemLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand and command lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_cue handler for '{}'.", name),
            )),
            Some(ItemLifecycle::DemandMap(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand and demand-map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand and value lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand and map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Http(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand and HTTP lane event handlers.",
                    name
                ),
            )),
            _ => {
                lane_lifecycles.insert(
                    name.clone(),
                    ItemLifecycle::Demand(DemandLifecycleDescriptor::new(
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
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand and value lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::DemandMap(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and value lane event handlers.",
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
            Some(ItemLifecycle::Http(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both value and HTTP lane event handlers.",
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
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand and value lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::DemandMap(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and value lane event handlers.",
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
            Some(ItemLifecycle::Http(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both value and HTTP lane event handlers.",
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
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand and map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::DemandMap(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and map lane event handlers.",
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
            Some(ItemLifecycle::Http(_)) => Err(syn::Error::new_spanned(
                method,
                format!("Lane '{}' has both map and HTTP lane event handlers.", name),
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
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand and map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::DemandMap(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and map lane event handlers.",
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
            Some(ItemLifecycle::Http(_)) => Err(syn::Error::new_spanned(
                method,
                format!("Lane '{}' has both map and HTTP lane event handlers.", name),
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
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand and map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::DemandMap(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and map lane event handlers.",
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
            Some(ItemLifecycle::Http(_)) => Err(syn::Error::new_spanned(
                method,
                format!("Lane '{}' has both map and HTTP lane event handlers.", name),
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

    pub fn add_demand_map_keys(
        &mut self,
        name: String,
        key_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(ItemLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and command lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and demand lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::DemandMap(desc)) => desc.add_keys(key_type, method),
            Some(ItemLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and value lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Http(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and HTTP lane event handlers.",
                    name
                ),
            )),
            _ => {
                lane_lifecycles.insert(
                    name.clone(),
                    ItemLifecycle::DemandMap(DemandMapLifecycleDescriptor::new_keys(
                        name, key_type, method,
                    )),
                );
                Ok(())
            }
        }
    }

    pub fn add_on_cue_key(
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
                    "Lane '{}' has both demand-map and command lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and demand lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::DemandMap(desc)) => {
                desc.add_on_cue_key(key_type, value_type, method)
            }
            Some(ItemLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and value lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Http(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both demand-map and HTTP lane event handlers.",
                    name
                ),
            )),
            _ => {
                lane_lifecycles.insert(
                    name.clone(),
                    ItemLifecycle::DemandMap(DemandMapLifecycleDescriptor::new_on_cue_key(
                        name, key_type, value_type, method,
                    )),
                );
                Ok(())
            }
        }
    }

    pub fn add_on_get(
        &mut self,
        name: String,
        get_type: Cow<'a, Type>,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(ItemLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and command lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and demand lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::DemandMap(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and demand-map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and value lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!("Lane '{}' has both HTTP and map lane event handlers.", name),
            )),
            Some(ItemLifecycle::Http(http_lifecycle)) => {
                http_lifecycle.add_on_get(get_type, method)
            }
            _ => {
                let mut lifecycle = HttpLifecycleDescriptor::new(name.clone());
                lifecycle.add_on_get(get_type, method)?;
                lane_lifecycles.insert(name, ItemLifecycle::Http(lifecycle));
                Ok(())
            }
        }
    }

    pub fn add_on_post(
        &mut self,
        name: String,
        post_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(ItemLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and command lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and demand lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::DemandMap(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and demand-map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and value lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!("Lane '{}' has both HTTP and map lane event handlers.", name),
            )),
            Some(ItemLifecycle::Http(http_lifecycle)) => {
                http_lifecycle.add_on_post(post_type, method)
            }
            _ => {
                let mut lifecycle = HttpLifecycleDescriptor::new(name.clone());
                lifecycle.add_on_post(post_type, method)?;
                lane_lifecycles.insert(name, ItemLifecycle::Http(lifecycle));
                Ok(())
            }
        }
    }

    pub fn add_on_put(
        &mut self,
        name: String,
        put_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(ItemLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and command lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and demand lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::DemandMap(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and demand-map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and value lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!("Lane '{}' has both HTTP and map lane event handlers.", name),
            )),
            Some(ItemLifecycle::Http(http_lifecycle)) => {
                http_lifecycle.add_on_put(put_type, method)
            }
            _ => {
                let mut lifecycle = HttpLifecycleDescriptor::new(name.clone());
                lifecycle.add_on_put(put_type, method)?;
                lane_lifecycles.insert(name, ItemLifecycle::Http(lifecycle));
                Ok(())
            }
        }
    }

    pub fn add_on_delete(&mut self, name: String, method: &'a Ident) -> Result<(), syn::Error> {
        let AgentLifecycleDescriptorBuilder {
            lane_lifecycles, ..
        } = self;
        match lane_lifecycles.get_mut(&name) {
            Some(ItemLifecycle::Command(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and command lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Demand(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and demand lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::DemandMap(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and demand-map lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Value(_)) => Err(syn::Error::new_spanned(
                method,
                format!(
                    "Lane '{}' has both HTTP and value lane event handlers.",
                    name
                ),
            )),
            Some(ItemLifecycle::Map(_)) => Err(syn::Error::new_spanned(
                method,
                format!("Lane '{}' has both HTTP and map lane event handlers.", name),
            )),
            Some(ItemLifecycle::Http(http_lifecycle)) => http_lifecycle.add_on_delete(method),
            _ => {
                let mut lifecycle = HttpLifecycleDescriptor::new(name.clone());
                lifecycle.add_on_delete(method)?;
                lane_lifecycles.insert(name, ItemLifecycle::Http(lifecycle));
                Ok(())
            }
        }
    }
}

/// Lifecycle attached to a single item.
pub enum ItemLifecycle<'a> {
    Value(ValueLifecycleDescriptor<'a>),
    Command(CommandLifecycleDescriptor<'a>),
    Demand(DemandLifecycleDescriptor<'a>),
    DemandMap(DemandMapLifecycleDescriptor<'a>),
    Http(HttpLifecycleDescriptor<'a>),
    Map(MapLifecycleDescriptor<'a>),
}

impl<'a> ItemLifecycle<'a> {
    fn item_name(&self) -> &str {
        let name = match self {
            ItemLifecycle::Value(ValueLifecycleDescriptor { name, .. }) => name,
            ItemLifecycle::Command(CommandLifecycleDescriptor { name, .. }) => name,
            ItemLifecycle::Demand(DemandLifecycleDescriptor { name, .. }) => name,
            ItemLifecycle::DemandMap(DemandMapLifecycleDescriptor { name, .. }) => name,
            ItemLifecycle::Map(MapLifecycleDescriptor { name, .. }) => name,
            ItemLifecycle::Http(HttpLifecycleDescriptor { name, .. }) => name,
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
            ItemLifecycle::Value(_) => {
                parse_quote! {
                    #root::agent_lifecycle::item_event::ValueLikeBranch
                }
            }
            ItemLifecycle::Command(_) => {
                parse_quote! {
                    #root::agent_lifecycle::item_event::CommandBranch
                }
            }
            ItemLifecycle::Demand(_) => {
                parse_quote! {
                    #root::agent_lifecycle::item_event::DemandBranch
                }
            }
            ItemLifecycle::DemandMap(_) => {
                parse_quote! {
                    #root::agent_lifecycle::item_event::DemandMapBranch
                }
            }
            ItemLifecycle::Http(_) => {
                parse_quote! {
                    #root::agent_lifecycle::item_event::HttpBranch
                }
            }
            ItemLifecycle::Map(_) => {
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
    pub name: String, //The name of the lane.
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

pub struct DemandLifecycleDescriptor<'a> {
    pub name: String, //The name of the lane.
    pub primary_lane_type: Option<&'a Type>,
    pub on_cue: &'a Ident,
}

impl<'a> DemandLifecycleDescriptor<'a> {
    pub fn new(name: String, primary_lane_type: Option<&'a Type>, on_cue: &'a Ident) -> Self {
        DemandLifecycleDescriptor {
            name,
            primary_lane_type,
            on_cue,
        }
    }
}

pub enum JoinLifecycle<'a> {
    None,
    JoinMap(&'a Type, &'a Ident),
    JoinValue(&'a Ident),
}

pub struct MapLifecycleDescriptor<'a> {
    name: String,                                          //The name of the lane.
    primary_lane_type: (&'a Type, &'a Type),               //First observed type of the lane.
    alternative_lane_types: HashSet<(&'a Type, &'a Type)>, //Further types observed for the lane.
    pub on_update: Option<&'a Ident>,
    pub on_remove: Option<&'a Ident>,
    pub on_clear: Option<&'a Ident>,
    pub join_lifecycle: JoinLifecycle<'a>,
}

pub struct HttpLifecycleDescriptor<'a> {
    name: String, //The name of the lane.
    get_type: Option<Cow<'a, Type>>,
    post_type: Option<&'a Type>,
    put_type: Option<&'a Type>,
    pub on_get: Option<&'a Ident>,
    pub on_post: Option<&'a Ident>,
    pub on_put: Option<&'a Ident>,
    pub on_delete: Option<&'a Ident>,
}

impl<'a> HttpLifecycleDescriptor<'a> {
    pub fn new(name: String) -> HttpLifecycleDescriptor<'a> {
        HttpLifecycleDescriptor {
            name,
            get_type: None,
            post_type: None,
            put_type: None,
            on_get: None,
            on_post: None,
            on_put: None,
            on_delete: None,
        }
    }

    pub fn add_on_get(
        &mut self,
        handler_type: Cow<'a, Type>,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let HttpLifecycleDescriptor {
            name,
            get_type,
            on_get,
            ..
        } = self;
        if on_get.is_some() {
            Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_get handler for '{}'.", name),
            ))
        } else {
            *get_type = Some(handler_type);
            *on_get = Some(method);
            Ok(())
        }
    }

    pub fn add_on_post(
        &mut self,
        handler_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let HttpLifecycleDescriptor {
            name,
            post_type,
            on_post,
            ..
        } = self;
        if on_post.is_some() {
            Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_post handler for '{}'.", name),
            ))
        } else {
            *post_type = Some(handler_type);
            *on_post = Some(method);
            Ok(())
        }
    }

    pub fn add_on_put(
        &mut self,
        handler_type: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let HttpLifecycleDescriptor {
            name,
            put_type,
            on_put,
            ..
        } = self;
        if on_put.is_some() {
            Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_put handler for '{}'.", name),
            ))
        } else {
            *put_type = Some(handler_type);
            *on_put = Some(method);
            Ok(())
        }
    }

    pub fn add_on_delete(&mut self, method: &'a Ident) -> Result<(), syn::Error> {
        let HttpLifecycleDescriptor {
            name, on_delete, ..
        } = self;
        if on_delete.is_some() {
            Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_delete handler for '{}'.", name),
            ))
        } else {
            *on_delete = Some(method);
            Ok(())
        }
    }
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
            join_lifecycle: JoinLifecycle::None,
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
            join_lifecycle: JoinLifecycle::None,
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
            join_lifecycle: JoinLifecycle::None,
        }
    }

    pub fn new_join_value_lifecycle(
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
            join_lifecycle: JoinLifecycle::JoinValue(join_lifecycle),
        }
    }

    pub fn new_join_map_lifecycle(
        name: String,
        link_key_type: &'a Type,
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
            join_lifecycle: JoinLifecycle::JoinMap(link_key_type, join_lifecycle),
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

    pub fn add_join_value_lifecycle(
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
        match join_lifecycle {
            JoinLifecycle::None => {
                *join_lifecycle = JoinLifecycle::JoinValue(method);
                if map_type != *primary_lane_type {
                    alternative_lane_types.insert(map_type);
                }
                Ok(())
            }
            _ => Err(syn::Error::new_spanned(
                method,
                format!("Duplicate join lane lifecycle for '{}'.", name),
            )),
        }
    }

    pub fn add_join_map_lifecycle(
        &mut self,
        link_key_type: &'a Type,
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
        match join_lifecycle {
            JoinLifecycle::None => {
                *join_lifecycle = JoinLifecycle::JoinMap(link_key_type, method);
                if map_type != *primary_lane_type {
                    alternative_lane_types.insert(map_type);
                }
                Ok(())
            }
            _ => Err(syn::Error::new_spanned(
                method,
                format!("Duplicate join lane lifecycle for '{}'.", name),
            )),
        }
    }
}

pub struct DemandMapLifecycleDescriptor<'a> {
    name: String,
    key_type: &'a Type,
    value_type: Option<&'a Type>,
    alternative_key_types: HashSet<&'a Type>,
    pub keys: Option<&'a Ident>,
    pub on_cue_key: Option<&'a Ident>,
}

impl<'a> DemandMapLifecycleDescriptor<'a> {
    pub fn new_keys(name: String, key_type: &'a Type, method: &'a Ident) -> Self {
        DemandMapLifecycleDescriptor {
            name,
            key_type,
            value_type: None,
            keys: Some(method),
            on_cue_key: None,
            alternative_key_types: HashSet::new(),
        }
    }

    pub fn new_on_cue_key(
        name: String,
        key_type: &'a Type,
        value_type: &'a Type,
        method: &'a Ident,
    ) -> Self {
        DemandMapLifecycleDescriptor {
            name,
            key_type,
            value_type: Some(value_type),
            keys: None,
            on_cue_key: Some(method),
            alternative_key_types: HashSet::new(),
        }
    }

    pub fn add_keys(&mut self, key_t: &'a Type, method: &'a Ident) -> Result<(), syn::Error> {
        let DemandMapLifecycleDescriptor {
            name,
            key_type,
            keys,
            alternative_key_types,
            ..
        } = self;
        if keys.is_some() {
            Err(syn::Error::new_spanned(
                method,
                format!("Duplicate keys handler for '{}'.", name),
            ))
        } else {
            if key_type != &key_t {
                alternative_key_types.insert(key_t);
            }
            *keys = Some(method);
            Ok(())
        }
    }

    pub fn add_on_cue_key(
        &mut self,
        key_t: &'a Type,
        value_t: &'a Type,
        method: &'a Ident,
    ) -> Result<(), syn::Error> {
        let DemandMapLifecycleDescriptor {
            name,
            key_type,
            value_type,
            on_cue_key,
            alternative_key_types,
            ..
        } = self;
        if on_cue_key.is_some() {
            Err(syn::Error::new_spanned(
                method,
                format!("Duplicate on_cue_key handler for '{}'.", name),
            ))
        } else {
            if key_type != &key_t {
                alternative_key_types.insert(key_t);
            }
            *value_type = Some(value_t);
            *on_cue_key = Some(method);
            Ok(())
        }
    }
}
