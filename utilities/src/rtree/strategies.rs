// Copyright 2015-2020 SWIM.AI inc.
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

use crate::rtree::{BoxBounded, EntryPtr, Label, Point, Rect};
use num::Float;
use std::cmp::Ordering;
use std::fmt::Debug;

/// The strategy that will be used to split the nodes of the [`RTree`](struct.RTree.html), once the maximum capacity is reached.
///
/// The two supported strategies run in linear and quadratic time.
#[derive(Debug, Clone, Copy)]
pub enum SplitStrategy {
    Linear,
    Quadratic,
}

pub(in crate) fn quadratic_pick_seeds<B, L>(entries: &[EntryPtr<B, L>]) -> (usize, usize)
where
    B: BoxBounded,
    L: Label,
{
    let mut first_idx = 0;
    let mut second_idx = 1;
    let mut max_diff = None;

    if entries.len() > 2 {
        for (i, first_item) in entries.iter().enumerate() {
            for (j, second_item) in entries.iter().enumerate().skip(i + 1) {
                let combined_rect = first_item.get_mbb().combine_boxes(second_item.get_mbb());
                let diff = combined_rect.measure()
                    - first_item.get_mbb().measure()
                    - second_item.get_mbb().measure();

                if let Some(max) = max_diff {
                    if diff > max {
                        max_diff = Some(diff);
                        first_idx = i;
                        second_idx = j;
                    }
                } else {
                    max_diff = Some(diff);
                }
            }
        }
    }

    (first_idx, second_idx)
}

pub(in crate) fn pick_next_quadratic<B, L>(
    entries: &[EntryPtr<B, L>],
    first_mbb: &Rect<B::Point>,
    second_mbb: &Rect<B::Point>,
    first_group_size: usize,
    second_group_size: usize,
) -> (usize, Rect<B::Point>, Group)
where
    B: BoxBounded,
    L: Label,
{
    let mut entries_iter = entries.iter();
    let item = entries_iter.next().unwrap();
    let mut item_idx = 0;

    let SplitPreference {
        first_preference,
        first_expanded_rect,
        second_preference,
        second_expanded_rect,
    } = calc_preferences(item, first_mbb, second_mbb);

    let mut max_preference_diff = (first_preference - second_preference).abs();

    let mut group = select_group::<B, L>(
        first_mbb,
        second_mbb,
        first_group_size,
        second_group_size,
        first_preference,
        second_preference,
    );

    let mut expanded_rect = match group {
        Group::First => first_expanded_rect,
        Group::Second => second_expanded_rect,
    };

    for (item, idx) in entries_iter.zip(1..) {
        let SplitPreference {
            first_preference,
            first_expanded_rect,
            second_preference,
            second_expanded_rect,
        } = calc_preferences(item, first_mbb, second_mbb);

        let preference_diff = (first_preference - second_preference).abs();

        if max_preference_diff <= preference_diff {
            max_preference_diff = preference_diff;
            item_idx = idx;

            group = select_group::<B, L>(
                first_mbb,
                second_mbb,
                first_group_size,
                second_group_size,
                first_preference,
                second_preference,
            );

            expanded_rect = match group {
                Group::First => first_expanded_rect,
                Group::Second => second_expanded_rect,
            };
        }
    }

    (item_idx, expanded_rect, group)
}

type PointType<B> = <<B as BoxBounded>::Point as Point>::Type;

pub(in crate) fn linear_pick_seeds<B, L>(entries: &[EntryPtr<B, L>]) -> (usize, usize)
where
    B: BoxBounded,
    L: Label,
{
    let mut first_idx = 0;
    let mut second_idx = 1;

    let mut dim_boundary_points: Vec<(PointType<B>, PointType<B>)> = vec![];
    let mut max_low_sides: Vec<(usize, PointType<B>)> = vec![];
    let mut min_high_sides: Vec<(usize, PointType<B>)> = vec![];

    if entries.len() > 2 {
        for (i, item) in entries.iter().enumerate() {
            let mbb = item.get_mbb();

            for dim in 0..mbb.get_coord_count() {
                let low_dim = mbb.low.get_nth_coord(dim).unwrap();
                let high_dim = mbb.high.get_nth_coord(dim).unwrap();

                match dim_boundary_points.get_mut(dim) {
                    Some((min_low, max_high)) => {
                        if low_dim < *min_low {
                            *min_low = low_dim
                        }

                        if high_dim > *max_high {
                            *max_high = high_dim
                        }
                    }
                    None => dim_boundary_points.push((low_dim, high_dim)),
                }

                match max_low_sides.get_mut(dim) {
                    Some((idx, max_low_dim)) => {
                        if low_dim > *max_low_dim {
                            *idx = i;
                            *max_low_dim = low_dim
                        }
                    }
                    None => max_low_sides.push((i, low_dim)),
                }

                match min_high_sides.get_mut(dim) {
                    Some((idx, min_high_dim)) => {
                        if high_dim < *min_high_dim {
                            *idx = i;
                            *min_high_dim = high_dim
                        }
                    }
                    None => min_high_sides.push((i, high_dim)),
                }
            }
        }

        let dim_lengths: Vec<_> = dim_boundary_points
            .into_iter()
            .map(|(low, high)| (high - low).abs())
            .collect();

        let side_separations: Vec<_> = max_low_sides
            .into_iter()
            .zip(min_high_sides.into_iter())
            .map(|((idx_low, low), (idx_high, high))| (idx_low, idx_high, (high - low).abs()))
            .collect();

        let normalised_separations: Vec<_> = side_separations
            .into_iter()
            .zip(dim_lengths.into_iter())
            .map(|((f, s, separation), dim_len)| (f, s, separation / dim_len))
            .collect();

        let max_separation = normalised_separations
            .into_iter()
            .max_by(|(_, _, norm_sep_1), (_, _, norm_sep_2)| {
                norm_sep_1.partial_cmp(norm_sep_2).unwrap()
            })
            .unwrap();

        first_idx = max_separation.0;
        second_idx = max_separation.1;
    }

    let (first_idx, second_idx) = match first_idx.cmp(&second_idx) {
        Ordering::Less => (first_idx, second_idx),
        Ordering::Greater => (second_idx, first_idx),
        Ordering::Equal if first_idx == 0 => (first_idx, 1),
        Ordering::Equal => (0, second_idx),
    };

    (first_idx, second_idx)
}

pub(in crate) fn pick_next_linear<B, L>(
    entries: &[EntryPtr<B, L>],
    mbb: &Rect<B::Point>,
) -> (usize, Rect<B::Point>, Group)
where
    B: BoxBounded,
    L: Label,
{
    (
        0,
        mbb.combine_boxes(entries.get(0).unwrap().get_mbb()),
        Group::First,
    )
}

struct SplitPreference<B, T> {
    first_preference: T,
    first_expanded_rect: B,
    second_preference: T,
    second_expanded_rect: B,
}

fn calc_preferences<B, L>(
    item: &EntryPtr<B, L>,
    first_mbb: &Rect<B::Point>,
    second_mbb: &Rect<B::Point>,
) -> SplitPreference<Rect<B::Point>, PointType<B>>
where
    B: BoxBounded,
    L: Label,
{
    let first_expanded_rect = first_mbb.combine_boxes(item.get_mbb());
    let first_diff = first_expanded_rect.measure() - first_mbb.measure();

    let second_expanded_rect = second_mbb.combine_boxes(item.get_mbb());
    let second_diff = second_expanded_rect.measure() - second_mbb.measure();

    SplitPreference {
        first_preference: first_diff,
        first_expanded_rect,
        second_preference: second_diff,
        second_expanded_rect,
    }
}

fn select_group<B, L>(
    first_mbb: &Rect<B::Point>,
    second_mbb: &Rect<B::Point>,
    first_group_size: usize,
    second_group_size: usize,
    first_diff: <B::Point as Point>::Type,
    second_diff: <B::Point as Point>::Type,
) -> Group
where
    B: BoxBounded,
{
    if first_diff < second_diff {
        Group::First
    } else if second_diff < first_diff {
        Group::Second
    } else if first_mbb.measure() < second_mbb.measure() {
        Group::First
    } else if second_mbb.measure() < first_mbb.measure() {
        Group::Second
    } else if first_group_size < second_group_size {
        Group::First
    } else if second_group_size < first_group_size {
        Group::Second
    } else {
        Group::First
    }
}

pub(in crate) enum Group {
    First,
    Second,
}
