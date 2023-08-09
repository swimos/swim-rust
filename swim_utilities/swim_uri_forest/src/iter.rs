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

use crate::TreeNode;
use smol_str::SmolStr;
use std::collections::{HashMap, VecDeque};

pub struct PathSegmentIterator<'a> {
    path: &'a str,
}

impl<'a> PathSegmentIterator<'a> {
    pub(crate) fn new(mut path: &'a str) -> PathSegmentIterator<'a> {
        while path.starts_with('/') {
            path = &path[1..];
        }

        PathSegmentIterator { path }
    }
}

impl<'a> Iterator for PathSegmentIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let PathSegmentIterator { path } = self;

        let lower = path.find(|c| c != '/')?;
        let upper = path[lower..]
            .find('/')
            .map_or(path.len(), |next_slash| lower + next_slash);

        let segment = Some(&path[lower..upper]);

        *path = &path[upper..];

        segment
    }
}

pub struct UriForestIterator<'l, D> {
    /// A prefix that is prepended to each yielded path.
    prefix: String,
    /// A stack of nodes to visit.
    visit: VecDeque<(&'l SmolStr, &'l TreeNode<D>)>,
    /// A stack containing the current path that is being built.
    uri_stack: VecDeque<String>,
    /// A stack of searches that are being performed and a cursor signalling the depth.
    op_stack: VecDeque<usize>,
}

impl<'l, D> UriForestIterator<'l, D> {
    pub(crate) fn empty() -> UriForestIterator<'l, D> {
        UriForestIterator {
            prefix: "".to_string(),
            visit: VecDeque::default(),
            uri_stack: VecDeque::default(),
            op_stack: VecDeque::default(),
        }
    }

    pub(crate) fn new(
        prefix: String,
        nodes: &'l HashMap<SmolStr, TreeNode<D>>,
    ) -> UriForestIterator<'l, D> {
        UriForestIterator {
            prefix,
            visit: VecDeque::from_iter(nodes),
            uri_stack: VecDeque::default(),
            op_stack: VecDeque::new(),
        }
    }
}

impl<'l, D> Iterator for UriForestIterator<'l, D> {
    type Item = (String, &'l D);

    /// Performs a depth-first search of the tree, yielding every node that contains data (signals
    /// the end of a path).
    fn next(&mut self) -> Option<Self::Item> {
        let UriForestIterator {
            prefix,
            visit,
            uri_stack,
            op_stack,
        } = self;

        loop {
            if visit.is_empty() {
                assert!(op_stack.is_empty());
                return None;
            }

            while let Some((current_segment, node)) = visit.pop_front() {
                uri_stack.push_back(current_segment.to_string());
                op_stack.push_front(node.descendants.len());

                let ret = node.data.as_ref().map(|data| {
                    let suffix = uri_stack.iter().cloned().collect::<Vec<String>>().join("/");
                    (format!("{}/{}", prefix, suffix), data)
                });

                dfs(node, visit, uri_stack, op_stack);

                if let Some(ret) = ret {
                    return Some(ret);
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum UriPart<'l, D> {
    Leaf { path: String, data: &'l D },
    Junction { path: String, descendants: usize },
}

pub struct UriPartIterator<'l, D> {
    /// A stack of nodes to visit.
    visit: VecDeque<(&'l SmolStr, &'l TreeNode<D>)>,
    /// A stack containing the current path that is being built.
    uri_stack: VecDeque<String>,
    /// A stack of searches that are being performed and a cursor signalling the depth.
    op_stack: VecDeque<usize>,
}

impl<'l, D> UriPartIterator<'l, D> {
    pub(crate) fn new(nodes: &'l HashMap<SmolStr, TreeNode<D>>) -> UriPartIterator<'l, D> {
        UriPartIterator {
            visit: VecDeque::from_iter(nodes),
            uri_stack: Default::default(),
            op_stack: Default::default(),
        }
    }
}

impl<'l, D> Iterator for UriPartIterator<'l, D> {
    type Item = UriPart<'l, D>;

    fn next(&mut self) -> Option<Self::Item> {
        let UriPartIterator {
            visit,
            uri_stack,
            op_stack,
        } = self;

        loop {
            if visit.is_empty() {
                assert!(op_stack.is_empty());
                return None;
            }

            while let Some((current_segment, node)) = visit.pop_front() {
                uri_stack.push_back(current_segment.to_string());
                op_stack.push_front(node.descendants.len());

                let make_uri = || {
                    let suffix = uri_stack.iter().cloned().collect::<Vec<String>>().join("/");
                    format!("/{}", suffix)
                };

                let ret = if node.has_descendants() {
                    let ret = Some(UriPart::Junction {
                        path: make_uri(),
                        descendants: node.descendants_len(),
                    });
                    uri_stack.pop_back();
                    op_stack.pop_front();
                    ret
                } else {
                    let ret = node.data.as_ref().map(|data| UriPart::Leaf {
                            path: make_uri(),
                            data,
                        });
                    dfs(node, visit, uri_stack, op_stack);
                    ret
                };

                if let Some(ret) = ret {
                    return Some(ret);
                }
            }
        }
    }
}

/// Performs a depth-first search from 'node'. Populating the visit stack with the next nodes to
/// visit or if there are no reachable nodes from 'node', then drains the URI stack back up to the
/// next node to visit.
fn dfs<'l, D>(
    node: &'l TreeNode<D>,
    visit_stack: &mut VecDeque<(&'l SmolStr, &'l TreeNode<D>)>,
    uri_stack: &mut VecDeque<String>,
    op_stack: &mut VecDeque<usize>,
) {
    if node.has_descendants() {
        // Insert the next collection of nodes to search
        for (key, descendant) in &node.descendants {
            visit_stack.push_front((key, descendant));
        }
    } else {
        // Drains any path segments that are no longer required.
        while let Some(remaining) = op_stack.front_mut() {
            if *remaining > 0 {
                *remaining -= 1;
                // This segment is now complete. We want to update decrement
                // indices in the callstack and remove any unrequired nodes.
                if *remaining == 0 {
                    uri_stack.pop_back();
                    op_stack.pop_front();
                } else {
                    // This node is going to be used as part of another path.
                    break;
                }
            } else {
                // This callstack was the only route to the node (i.e, it had no other
                // children) so it can be removed

                uri_stack.pop_back();
                op_stack.pop_front();
            }
        }
    }
}
