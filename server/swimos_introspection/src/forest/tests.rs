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

use std::collections::{HashMap, HashSet};

use crate::forest::{
    iter::{PathSegmentIterator, UriPart},
    TreeNode, UriForest,
};

#[test]
fn iters() {
    let mut forest = UriForest::new();

    forest.insert("/unit/1/cnt/s/1", ());
    forest.insert("/unit/1/cnt/2", ());
    forest.insert("/unit/1/blah/", ());

    forest.insert("/unit/2/cnt/1", ());
    forest.insert("/unit/3/cnt/1", ());
    forest.insert("/unit/4/cnt/1", ());

    forest.insert("/listener/1", ());
    forest.insert("/listener/2", ());

    let uris = forest
        .prefix_iter("/unit/1")
        .map(|(uri, _)| uri)
        .collect::<HashSet<String>>();

    assert_eq!(
        uris,
        HashSet::from([
            "/unit/1/cnt/2".to_string(),
            "/unit/1/cnt/s/1".to_string(),
            "/unit/1/blah".to_string(),
        ])
    );

    let uris = forest
        .prefix_iter("/unit/2")
        .map(|(uri, _)| uri)
        .collect::<HashSet<String>>();
    assert_eq!(uris, HashSet::from(["/unit/2/cnt/1".to_string()]));

    let uris = forest
        .prefix_iter("/listener")
        .map(|(uri, _)| uri)
        .collect::<HashSet<String>>();
    assert_eq!(
        uris,
        HashSet::from(["/listener/1".to_string(), "/listener/2".to_string()])
    );

    let all_uris = forest
        .uri_iter()
        .map(|(uri, _)| uri)
        .collect::<HashSet<String>>();
    assert_eq!(
        all_uris,
        HashSet::from([
            "/unit/4/cnt/1".to_string(),
            "/unit/3/cnt/1".to_string(),
            "/unit/2/cnt/1".to_string(),
            "/unit/1/blah".to_string(),
            "/unit/1/cnt/2".to_string(),
            "/unit/1/cnt/s/1".to_string(),
            "/listener/2".to_string(),
            "/listener/1".to_string()
        ])
    );
}

// Tests that path segments are entered and exited correctly in the iterator, particularly at
// junction nodes - such as /cnt/s/1
#[test]
fn iters2() {
    let mut forest = UriForest::new();

    forest.insert("/unit/1/cnt/s/1/a/b/c/d/e/f/g", ());
    forest.insert("/unit/1/cnt/s/1/2/3/4/5/6/7/8", ());
    forest.insert("/unit/1/cnt/s/1/2/3", ());
    forest.insert("/unit/1/cnt/2/h/i/j/k", ());
    forest.insert("/unit/1/blah/", ());

    let uris = forest
        .prefix_iter("/unit/1")
        .map(|(uri, _)| uri)
        .collect::<HashSet<String>>();
    assert_eq!(
        uris,
        HashSet::from([
            "/unit/1/cnt/2/h/i/j/k".to_string(),
            "/unit/1/cnt/s/1/2/3".to_string(),
            "/unit/1/cnt/s/1/2/3/4/5/6/7/8".to_string(),
            "/unit/1/cnt/s/1/a/b/c/d/e/f/g".to_string(),
            "/unit/1/blah".to_string(),
        ])
    );
}

#[test]
fn iter3() {
    let mut forest = UriForest::new();

    forest.insert("/unit/1/a/b/c", ());
    forest.insert("/unit/1/a/d/e", ());
    forest.insert("/unit/1/blah/", ());

    let uris = forest
        .prefix_iter("/unit/1")
        .map(|(uri, _)| uri)
        .collect::<HashSet<String>>();
    assert_eq!(
        uris,
        HashSet::from([
            "/unit/1/a/d/e".to_string(),
            "/unit/1/a/b/c".to_string(),
            "/unit/1/blah".to_string(),
        ])
    );
}

#[test]
fn contains() {
    let mut forest = UriForest::new();
    forest.insert("/unit/1/cnt/3", ());

    // Assert that the node is not a leaf
    assert!(forest.contains_uri("/unit/1/cnt/3"));
    assert!(forest.contains_uri("/unit/1/cnt/3/"));

    // Assert that junction nodes and non-exist nodes all return false
    {
        assert!(!forest.contains_uri("/"));
        assert!(!forest.contains_uri("/unit/"));
        assert!(!forest.contains_uri("/unit/1"));
        assert!(!forest.contains_uri("/unit/1/cnt"));
        assert!(!forest.contains_uri("/unit/1/cnt/33"));
        assert!(!forest.contains_uri("/unit/1/cnt/3/3/3"));
    }

    assert_eq!(forest.remove("/unit/1/cnt/3/"), Some(()));
    assert!(!forest.contains_uri("/unit/1/cnt/3"));

    // Insert a new branch at /unit/
    forest.insert("/unit/2/cnt/4", ());
    assert!(forest.contains_uri("/unit/2/cnt/4"));
    assert!(!forest.contains_uri("/unit/3/cnt/"));

    assert_eq!(forest.remove("/unit/2/cnt/4/"), Some(()));
    assert!(!forest.contains_uri("/unit/2/cnt/4"));

    assert!(forest.is_empty());
}

#[test]
fn empty_after_remove() {
    let mut forest = UriForest::new();
    forest.insert("/unit/1/cnt/3", ());
    assert_eq!(forest.remove("/unit/1/cnt/3"), Some(()));
    assert!(forest.is_empty());
}

#[test]
fn update() {
    let mut forest = UriForest::new();
    forest.insert("/unit/1/cnt/3", 2);
    forest.insert("/unit/1/cnt/3", 20);
    assert_eq!(forest.remove("/unit/1/cnt/3"), Some(20));
    assert!(forest.is_empty());
}

#[test]
fn insert_remove_multiple() {
    let mut forest = UriForest::new();
    forest.insert("/unit/1/cnt/2", 1);
    forest.insert("/unit/1/cnt/3", 2);
    forest.insert("/unit/2/cnt/4", 3);
    forest.insert("/listener", 4);

    // Remove, insert, update, then remove the same URI
    assert_eq!(forest.remove("/unit/1/cnt/3"), Some(2));
    forest.insert("/unit/1/cnt/3", 20);
    forest.insert("/unit/1/cnt/3", 21);
    assert_eq!(forest.remove("/unit/1/cnt/3"), Some(21));

    forest.insert("/listener2", 5);
    forest.insert("/listener3", 6);
    forest.insert("/listener4", 7);
    forest.insert("/listener5", 8);

    assert_eq!(forest.remove("/listener"), Some(4));
    assert_eq!(forest.remove("/listener2"), Some(5));
    assert_eq!(forest.remove("/listener3"), Some(6));
    assert_eq!(forest.remove("/listener4"), Some(7));
    assert_eq!(forest.remove("/listener5"), Some(8));
    assert_eq!(forest.remove("/unit/1/cnt/2"), Some(1));
    assert_eq!(forest.remove("/unit/2/cnt/4"), Some(3));

    assert!(forest.is_empty());
}

#[test]
fn remove() {
    let mut forest = UriForest::new();
    forest.insert("/unit/1/cnt/2", 1);
    forest.insert("/unit/1/cnt/3", 2);
    forest.insert("/unit/2/cnt/4", 3);
    forest.insert("/listener", 4);

    assert_eq!(forest.remove("/unit/1/cnt/2"), Some(1));
    assert_eq!(forest.remove("/unit/1/cnt/3"), Some(2));
    assert_eq!(forest.remove("/unit/2/cnt/4"), Some(3));
    assert_eq!(forest.remove("/listener"), Some(4));
    assert_eq!(forest.remove("/listenerr"), None);
    assert_eq!(forest.remove("/"), None);

    assert!(forest.is_empty());
}

#[test]
fn insert() {
    let mut forest = UriForest::new();
    forest.insert("/unit/1/cnt/2", ());
    forest.insert("/unit/1/cnt/3", ());
    forest.insert("/unit/2/cnt/4", ());
    forest.insert("/listener", ());

    let expected = HashMap::from([
        (
            "unit".into(),
            TreeNode {
                data: None,
                descendants: HashMap::from([
                    (
                        "1".into(),
                        TreeNode {
                            data: None,
                            descendants: HashMap::from([(
                                "cnt".into(),
                                TreeNode {
                                    data: None,
                                    descendants: HashMap::from([
                                        (
                                            "2".into(),
                                            TreeNode {
                                                data: Some(()),
                                                descendants: Default::default(),
                                            },
                                        ),
                                        (
                                            "3".into(),
                                            TreeNode {
                                                data: Some(()),
                                                descendants: Default::default(),
                                            },
                                        ),
                                    ]),
                                },
                            )]),
                        },
                    ),
                    (
                        "2".into(),
                        TreeNode {
                            data: None,
                            descendants: HashMap::from([(
                                "cnt".into(),
                                TreeNode {
                                    data: None,
                                    descendants: HashMap::from([(
                                        "4".into(),
                                        TreeNode {
                                            data: Some(()),
                                            descendants: Default::default(),
                                        },
                                    )]),
                                },
                            )]),
                        },
                    ),
                ]),
            },
        ),
        (
            "listener".into(),
            TreeNode {
                data: Some(()),
                descendants: Default::default(),
            },
        ),
    ]);
    assert_eq!(forest.trees, expected);
}

#[test]
fn get() {
    let mut forest = UriForest::new();

    forest.insert("/unit/1/cnt/1", 1);
    forest.insert("/unit/2/cnt/2", 2);
    forest.insert("/unit/3/cnt/3", 3);
    forest.insert("/unit/4/cnt/4", 4);
    forest.insert("/unit/5/cnt/5", 5);

    assert_eq!(forest.get("/unit/3/cnt/3"), Some(&3));
    assert_eq!(forest.get_mut("/unit/3/cnt/3"), Some(&mut 3));

    assert_eq!(forest.get("/unit/3/cnt/33"), None);
    assert_eq!(forest.get_mut("/unit/3/cnt/33"), None);
}

#[test]
fn path_segment_iter() {
    let segments = PathSegmentIterator::new("/a/b/c").collect::<Vec<_>>();
    assert_eq!(segments, vec!["a", "b", "c"]);

    let segments = PathSegmentIterator::new("a/b/c//").collect::<Vec<_>>();
    assert_eq!(segments, vec!["a", "b", "c"]);

    let segments = PathSegmentIterator::new("////a///b///c").collect::<Vec<_>>();
    assert_eq!(segments, vec!["a", "b", "c"]);

    let segments = PathSegmentIterator::new("////").collect::<Vec<_>>();
    assert_eq!(segments, Vec::<&str>::new());
}

#[test]
fn uri_part_iter() {
    let mut forest = UriForest::new();

    forest.insert("/listener", ());
    forest.insert("/unit/1/cnt/", ());
    forest.insert("/unit/1/cnt/3", ());
    forest.insert("/unit/2/cnt/3", ());
    forest.insert("/unit/3/cnt/3/4", ());

    let actual = forest.part_iter().collect::<HashSet<_>>();
    let expected = HashSet::from([
        UriPart::Leaf {
            path: "/listener".to_string(),
            data: &(),
        },
        UriPart::Junction {
            path: "/unit".to_string(),
            descendants: 3,
        },
    ]);

    assert_eq!(actual, expected)
}
