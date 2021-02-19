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
///
/// ## Reference
/// For more information refer to ["R-trees: a dynamic index structure for spatial searching"](http://www-db.deis.unibo.it/courses/SI-LS/papers/Gut84.pdf).
#[derive(Debug, Clone, Copy)]
pub enum SplitStrategy {
    /// This algorithm is linear in the number of entries and number of dimensions.
    Linear,
    /// This algorithm attempts to find a small-area split, but is not guaranteed to find one with the smallest area possible.
    /// The cost is quadratic in the number of entries and liner in the number of dimensions.
    Quadratic,
}

// The algorithm picks two of the M + 1 entries to be the first elements of the two new groups
// by choosing the pair that would waste the most area if both were put in the same group,
// i.e. the area of a rectangle covering both entries, minus the areas of the entries
// themselves, would be greatest.
pub(in crate) fn quadratic_pick_seeds<L, B>(entries: &[EntryPtr<L, B>]) -> (usize, usize)
where
    L: Label,
    B: BoxBounded,
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

// The entries are assigned to groups one at a time. At each step the area expansion
// required to add each remaining entry to each group is calculated, and the entry
// assigned is the one showing the greatest difference between the two groups.
pub(in crate) fn pick_next_quadratic<L, B>(
    entries: &[EntryPtr<L, B>],
    first_mbb: &Rect<B::Point>,
    second_mbb: &Rect<B::Point>,
    first_group_size: usize,
    second_group_size: usize,
) -> (usize, Rect<B::Point>, Group)
where
    L: Label,
    B: BoxBounded,
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

    let mut group = select_group::<L, B>(
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

            group = select_group::<L, B>(
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

// The algorithm picks the two most extreme rectangles to be the first elements of the two
// new groups. For every dimension it finds the entry with the highest low side, and the
// entry with the lowest high side and records the separation. The separations are normalised
// by dividing by the width of the entire set along the corresponding dimension. It then
// chooses the pair with the greatest normalised separation along any dimension.
pub(in crate) fn linear_pick_seeds<L, B>(entries: &[EntryPtr<L, B>]) -> (usize, usize)
where
    L: Label,
    B: BoxBounded,
{
    let mut first_idx = 0;
    let mut second_idx = 1;

    let mut dim_boundary_points: Vec<(PointType<B>, PointType<B>)> = vec![];
    let mut max_low_sides: Vec<(usize, PointType<B>)> = vec![];
    let mut min_high_sides: Vec<(usize, PointType<B>)> = vec![];

    if entries.len() > 2 {
        for (i, item) in entries.iter().enumerate() {
            let mbb = item.get_mbb();

            for dim in 0..B::get_coord_type() as usize {
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

        let dim_lengths = dim_boundary_points
            .iter()
            .map(|(low, high)| (*high - *low).abs());

        let side_separations = max_low_sides
            .iter()
            .zip(min_high_sides.iter())
            .map(|((idx_low, low), (idx_high, high))| (idx_low, idx_high, (*high - *low).abs()));

        let normalised_separations = side_separations
            .zip(dim_lengths)
            .map(|((f, s, separation), dim_len)| (f, s, separation / dim_len));

        let max_separation = normalised_separations
            .max_by(|(_, _, norm_sep_1), (_, _, norm_sep_2)| {
                norm_sep_1
                    .partial_cmp(norm_sep_2)
                    .unwrap_or(Ordering::Equal)
            })
            .unwrap_or((
                &0,
                &1,
                <<B as BoxBounded>::Point as Point>::Type::min_value(),
            ));

        first_idx = *max_separation.0;
        second_idx = *max_separation.1;
    }

    let (first_idx, second_idx) = match first_idx.cmp(&second_idx) {
        Ordering::Less => (first_idx, second_idx),
        Ordering::Greater => (second_idx, first_idx),
        Ordering::Equal if first_idx == 0 => (first_idx, 1),
        Ordering::Equal => (0, second_idx),
    };

    (first_idx, second_idx)
}

// The entries are assigned to the first available group.
pub(in crate) fn pick_next_linear<L, B>(
    entries: &[EntryPtr<L, B>],
    mbb: &Rect<B::Point>,
) -> (usize, Rect<B::Point>, Group)
where
    L: Label,
    B: BoxBounded,
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

fn calc_preferences<L, B>(
    item: &EntryPtr<L, B>,
    first_mbb: &Rect<B::Point>,
    second_mbb: &Rect<B::Point>,
) -> SplitPreference<Rect<B::Point>, PointType<B>>
where
    L: Label,
    B: BoxBounded,
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

fn select_group<L, B>(
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
