// Copyright 2015-2021 SWIM.AI inc.
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

use num::Float;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Sub;

/// An n-dimensional rectangle defined by two points.
///
/// The number of dimensions are dictated by the type of the points that are used.
/// The low point is the point that has the lowest coordinates in all dimensions and the highest
/// point is the point has the highest coordinates in all dimensions.
///
/// For example, in a 2D rectangle, the low point is the lower-left corner and the high point
/// is the upper-right corner.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Rect<P>
where
    P: Point,
{
    pub(in crate) low: P,
    pub(in crate) high: P,
}

impl<P> Rect<P>
where
    P: Point,
{
    /// Creates an n-dimensional rectangle from two points.
    ///
    /// The first point must be the lowest point (in all dimensions) of the hyperrecatngle, and the second
    /// point must be the highest point (in all dimensions) of the hyperrecatngle.
    /// The high point must be strictly higher than the low point, in all dimensions.
    ///
    /// # Macro:
    /// Rectangles that use [`Point2D`](struct.Point2D.html) or [`Point3D`](struct.Point3D.html) can also be crated using the [`rect`](../macro.rect.html) macro,
    /// subject to the same restrictions described above.
    /// # Examples:
    /// ```
    /// use swim_rtree::{Rect, Point2D, rect, Point3D};
    ///
    /// let rect_2d = Rect::new(Point2D::new(0.0, 0.0), Point2D::new(1.0, 1.0));
    /// let rect_3d = Rect::new(Point3D::new(0.0, 0.0, 0.0), Point3D::new(1.0, 1.0, 1.0));
    ///
    /// let macro_rect_2d = rect!((0.0, 0.0), (1.0, 1.0));
    /// let macro_rect_3d = rect!((0.0, 0.0, 0.0), (1.0, 1.0, 1.0));
    ///
    /// assert_eq!(rect_2d, macro_rect_2d);
    /// assert_eq!(rect_3d, macro_rect_3d);
    /// ```
    ///
    /// # Panics:
    /// If the high point is not strictly higher than the low in all dimensions, the code will panic.
    /// ```should_panic
    /// # use swim_rtree::{Rect, Point2D};
    /// #
    /// // The low point has higher x coordinate
    /// Rect::new(Point2D::new(2.0, 0.0), Point2D::new(1.0, 1.0));
    /// ```
    ///
    /// ```should_panic
    /// # use swim_rtree::{Rect, Point2D, Point3D};
    /// #
    /// // The low and high points have equal z coordinate
    /// Rect::new(Point3D::new(0.0, 0.0, 10.0), Point3D::new(1.0, 1.0, 10.0));
    /// ```
    ///
    /// ```should_panic
    /// # use swim_rtree::{Rect, rect, Point2D};
    /// #
    /// // The low point has higher y coordinate
    /// rect!((0.0, 5.0), (1.0, 1.0));
    /// ```
    pub fn new(low: P, high: P) -> Self {
        assert!(
            low.partial_cmp(&high) == Some(Ordering::Less) && !low.has_any_matching_coords(&high),
            "The first point must be lower than the second."
        );

        Rect { low, high }
    }
}

impl<P> Rect<P>
where
    P: Point,
{
    /// Calculates a minimum bounding box that contains both items.
    pub(in crate) fn combine_boxes<B: BoxBounded<Point = <Self as BoxBounded>::Point>>(
        &self,
        other: &B,
    ) -> Rect<P> {
        let other_mbb = other.get_mbb();

        let new_low = self.low.get_lowest(&other_mbb.low);
        let new_high = self.high.get_highest(&other_mbb.high);

        Rect::new(new_low, new_high)
    }

    /// Checks if a bounding box is completely covering another bounding box.
    pub(in crate) fn is_covering<B: BoxBounded<Point = <Self as BoxBounded>::Point>>(
        &self,
        other: &B,
    ) -> bool {
        let other_mbb = other.get_mbb();
        self.low <= other_mbb.low && self.high >= other_mbb.high
    }

    /// Checks if two bounding boxes are intersecting.
    pub(in crate) fn is_intersecting<B: BoxBounded<Point = <Self as BoxBounded>::Point>>(
        &self,
        other: &B,
    ) -> bool {
        let other_mbb = other.get_mbb();
        !(self.low > other_mbb.high || self.high < other_mbb.low)
    }
}

impl<P> BoxBounded for Rect<P>
where
    P: Point,
{
    type Point = P;

    fn get_mbb(&self) -> &Rect<P> {
        self
    }

    fn get_center(&self) -> P {
        self.high.mean(&self.low)
    }

    fn measure(&self) -> P::Type {
        self.high.sub(self.low).multiply_coord()
    }
}

/// A 2D Point with Float number coordinates.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Point2D<T: Float> {
    x: T,
    y: T,
}

impl<T: Float> PartialOrd for Point2D<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.x == other.x && self.y == other.y {
            Some(Ordering::Equal)
        } else if self.x >= other.x && self.y >= other.y {
            Some(Ordering::Greater)
        } else if self.x <= other.x && self.y <= other.y {
            Some(Ordering::Less)
        } else {
            None
        }
    }
}

impl<T: Float> Point2D<T> {
    /// Creates a new 2D Point from two coordinates, x an y.
    ///
    /// # Example:
    /// ```
    /// use swim_rtree::Point2D;
    /// Point2D::new(0.0, 10.0);
    /// ```
    pub fn new(x: T, y: T) -> Self {
        Point2D { x, y }
    }
}

impl<T: Float + Debug> Sub for Point2D<T> {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Point2D {
            x: self.x - rhs.x,
            y: self.y - rhs.y,
        }
    }
}

impl<T: Float + Debug> Point for Point2D<T> {
    type Type = T;

    fn get_coord_type() -> CoordType {
        CoordType::TwoDimensional
    }

    fn get_nth_coord(&self, n: usize) -> Option<T> {
        if n == 0 {
            Some(self.x)
        } else if n == 1 {
            Some(self.y)
        } else {
            None
        }
    }

    fn mean(&self, other: &Point2D<T>) -> Point2D<T> {
        Point2D {
            x: (self.x + other.x) / T::from(2).unwrap(),
            y: (self.y + other.y) / T::from(2).unwrap(),
        }
    }

    fn multiply_coord(&self) -> T {
        let result = self.x * self.y;
        assert!(result.is_finite());
        result
    }

    fn has_any_matching_coords(&self, other: &Self) -> bool {
        self.x == other.x || self.y == other.y
    }

    fn get_lowest(&self, other: &Self) -> Self {
        let new_lower_x = if self.x > other.x { other.x } else { self.x };
        let new_lower_y = if self.y > other.y { other.y } else { self.y };

        Point2D {
            x: new_lower_x,
            y: new_lower_y,
        }
    }

    fn get_highest(&self, other: &Self) -> Self {
        let new_higher_x = if self.x > other.x { self.x } else { other.x };
        let new_higher_y = if self.y > other.y { self.y } else { other.y };

        Point2D {
            x: new_higher_x,
            y: new_higher_y,
        }
    }
}

/// A 3D Point with Float number coordinates.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Point3D<T: Float> {
    x: T,
    y: T,
    z: T,
}

impl<T: Float> PartialOrd for Point3D<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.x == other.x && self.y == other.y && self.z == other.z {
            Some(Ordering::Equal)
        } else if self.x >= other.x && self.y >= other.y && self.z >= other.z {
            Some(Ordering::Greater)
        } else if self.x <= other.x && self.y <= other.y && self.z <= other.z {
            Some(Ordering::Less)
        } else {
            None
        }
    }
}

impl<T: Float> Point3D<T> {
    /// Creates a new 3D Point from two coordinates, x an y.
    ///
    /// # Example:
    /// ```
    /// use swim_rtree::Point3D;
    /// Point3D::new(3.5, 1.5, 15.5);
    /// ```
    pub fn new(x: T, y: T, z: T) -> Self {
        Point3D { x, y, z }
    }
}

impl<T: Float + Debug> Sub for Point3D<T> {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Point3D {
            x: self.x - rhs.x,
            y: self.y - rhs.y,
            z: self.z - rhs.z,
        }
    }
}

impl<T: Float + Debug> Point for Point3D<T> {
    type Type = T;

    fn get_coord_type() -> CoordType {
        CoordType::ThreeDimensional
    }

    fn get_nth_coord(&self, n: usize) -> Option<T> {
        if n == 0 {
            Some(self.x)
        } else if n == 1 {
            Some(self.y)
        } else if n == 2 {
            Some(self.z)
        } else {
            None
        }
    }

    fn mean(&self, other: &Self) -> Point3D<T> {
        Point3D {
            x: (self.x + other.x) / T::from(2).unwrap(),
            y: (self.y + other.y) / T::from(2).unwrap(),
            z: (self.z + other.z) / T::from(2).unwrap(),
        }
    }

    fn multiply_coord(&self) -> T {
        let result = self.x * self.y * self.z;
        assert!(result.is_finite());
        result
    }

    fn has_any_matching_coords(&self, other: &Self) -> bool {
        self.x == other.x || self.y == other.y || self.z == other.z
    }

    fn get_lowest(&self, other: &Self) -> Self {
        let new_lower_x = if self.x > other.x { other.x } else { self.x };
        let new_lower_y = if self.y > other.y { other.y } else { self.y };
        let new_lower_z = if self.z > other.z { other.z } else { self.z };

        Point3D {
            x: new_lower_x,
            y: new_lower_y,
            z: new_lower_z,
        }
    }

    fn get_highest(&self, other: &Self) -> Self {
        let new_higher_x = if self.x > other.x { self.x } else { other.x };
        let new_higher_y = if self.y > other.y { self.y } else { other.y };
        let new_higher_z = if self.z > other.z { self.z } else { other.z };

        Point3D {
            x: new_higher_x,
            y: new_higher_y,
            z: new_higher_z,
        }
    }
}

/// The type of the coordinates of the entries in the RTree.
/// The RTree can currently work with 2D and 3D objects only.
pub enum CoordType {
    TwoDimensional = 2,
    ThreeDimensional = 3,
}

/// A trait for implementing a custom point.
///
/// The associated type of the point must be a [`Float`] number.
pub trait Point: Copy + Clone + PartialEq + PartialOrd + Debug + Sub<Output = Self> {
    type Type: Float + Debug;

    /// Returns the type of the coordinates of the point.
    fn get_coord_type() -> CoordType;

    // Returns the n-th coordinate of the point, or none if the point
    // has less than n coordinates.
    fn get_nth_coord(&self, n: usize) -> Option<Self::Type>;

    /// Calculates the mean point between two points.
    fn mean(&self, other: &Self) -> Self;

    /// Multiplies all the coordinates of the point together.
    fn multiply_coord(&self) -> Self::Type;

    /// Checks if two points have at least one equal coordinate.
    fn has_any_matching_coords(&self, other: &Self) -> bool;

    /// Returns a point from the lowest coordinate of two points for each dimension.
    fn get_lowest(&self, other: &Self) -> Self;

    /// Returns a point from the highest coordinate of two points for each dimension.
    fn get_highest(&self, other: &Self) -> Self;
}

/// A trait for implementing custom objects that can be bound by a box.
///
/// The associated type of the box bounded object must be a [`Point`](trait.Point.html).
pub trait BoxBounded: Clone + Debug {
    type Point: Point;

    /// Returns the minimum bounding box.
    fn get_mbb(&self) -> &Rect<Self::Point>;

    /// Calculates the center of the bounding box of the object.
    fn get_center(&self) -> Self::Point;

    /// Returns the type of the coordinates of the point.
    fn get_coord_type() -> CoordType {
        Self::Point::get_coord_type()
    }

    /// Calculates the area for 2D objects and volume for 3D objects.
    fn measure(&self) -> <Self::Point as Point>::Type;
}

/// A trait for objects that can be used as labels of entries in the RTree.
pub trait Label: Hash + Eq + Debug + Clone {}

impl<T: Hash + Eq + Debug + Clone> Label for T {}

///Creates a [`Rect`](rtree/struct.Rect.html) from coordinates.
///
/// The macro supports the creation of rectangles using [`Point2D`](rtree/struct.Point2D.html) and [`Point3D`](rtree/struct.Point3D.html).
///
/// # Example:
/// ```
/// use swim_rtree::{Rect, Point2D, rect, Point3D};
///
/// let rect_2d = Rect::new(Point2D::new(0.0, 10.0), Point2D::new(1.0, 11.0));
/// let rect_3d = Rect::new(Point3D::new(10.0, 2.0, 15.0), Point3D::new(11.0, 3.0, 16.0));
///
/// let macro_rect_2d = rect!((0.0, 10.0), (1.0, 11.0));
/// let macro_rect_3d = rect!((10.0, 2.0, 15.0), (11.0, 3.0, 16.0));
///
/// assert_eq!(rect_2d, macro_rect_2d);
/// assert_eq!(rect_3d, macro_rect_3d);
/// ```
#[macro_export]
macro_rules! rect {
    ( ($low_x:expr, $low_y:expr), ($high_x:expr, $high_y:expr)) => {
        Rect::new(Point2D::new($low_x, $low_y), Point2D::new($high_x, $high_y))
    };

    ( ($low_x:expr, $low_y:expr, $low_z:expr), ($high_x:expr, $high_y:expr, $high_z:expr)) => {
        Rect::new(
            Point3D::new($low_x, $low_y, $low_z),
            Point3D::new($high_x, $high_y, $high_z),
        )
    };
}
