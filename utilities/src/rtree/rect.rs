use num::traits::real::Real;
use std::fmt::Debug;
use std::ops::Sub;

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Rect<P>
where
    P: Point,
{
    low: P,
    high: P,
}

impl<P> Rect<P>
where
    P: Point,
{
    pub fn new(low: P, high: P) -> Self {
        if low.has_higher_cords(&high) || low.has_equal_cords(&high) {
            panic!("The first point must be lower than the second.")
        }

        Rect { low, high }
    }

    pub fn get_low(&self) -> &P {
        &self.low
    }

    pub fn get_high(&self) -> &P {
        &self.high
    }
}

impl<P> BoundingBox for Rect<P>
where
    P: Point,
{
    type Point = P;

    fn get_mbb(&self) -> &Rect<P> {
        self
    }

    fn get_center(&self) -> P {
        self.get_high().mean(self.get_low())
    }

    fn measure(&self) -> P::Type {
        self.high.sub(self.low).multiply_coord()
    }

    fn combine_boxes<B: BoundingBox<Point = Self::Point>>(&self, other: &B) -> Rect<P> {
        let other_mbb = other.get_mbb();

        let new_low = self.low.get_lowest(&other_mbb.low);
        let new_high = self.high.get_highest(&other_mbb.high);

        Rect::new(new_low, new_high)
    }

    fn is_covering<B: BoundingBox<Point = Self::Point>>(&self, other: &B) -> bool {
        let other_mbb = other.get_mbb();
        self.low.has_lower_cords(&other_mbb.low) && self.high.has_higher_cords(&other_mbb.high)
    }

    fn is_intersecting<B: BoundingBox<Point = Self::Point>>(&self, other: &B) -> bool {
        let other_mbb = other.get_mbb();
        !self.low.has_higher_cords(&other_mbb.high) && !self.high.has_lower_cords(&other_mbb.low)
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Point2D<T: Real> {
    x: T,
    y: T,
}

impl<T: Real> Point2D<T> {
    pub fn new(x: T, y: T) -> Self {
        Point2D { x, y }
    }
}

impl<T: Real + Debug> Sub for Point2D<T> {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Point2D {
            x: self.x - rhs.x,
            y: self.y - rhs.y,
        }
    }
}

impl<T: Real + Debug> Point for Point2D<T> {
    type Type = T;

    fn get_coord_count() -> u32 {
        2
    }

    fn get_nth_coord(&self, n: u32) -> Option<T> {
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
        self.x * self.y
    }

    fn has_equal_cords(&self, other: &Self) -> bool {
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

    fn has_higher_cords(&self, other: &Self) -> bool {
        self.x >= other.x && self.y >= other.y
    }

    fn has_lower_cords(&self, other: &Self) -> bool {
        self.x <= other.x && self.y <= other.y
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Point3D<T: Real> {
    x: T,
    y: T,
    z: T,
}

impl<T: Real> Point3D<T> {
    pub fn new(x: T, y: T, z: T) -> Self {
        Point3D { x, y, z }
    }
}

impl<T: Real + Debug> Sub for Point3D<T> {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Point3D {
            x: self.x - rhs.x,
            y: self.y - rhs.y,
            z: self.z - rhs.z,
        }
    }
}

impl<T: Real + Debug> Point for Point3D<T> {
    type Type = T;

    fn get_coord_count() -> u32 {
        3
    }

    fn get_nth_coord(&self, n: u32) -> Option<T> {
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
        self.x * self.y * self.z
    }

    fn has_equal_cords(&self, other: &Self) -> bool {
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

    fn has_higher_cords(&self, other: &Self) -> bool {
        self.x >= other.x && self.y >= other.y && self.z >= other.z
    }

    fn has_lower_cords(&self, other: &Self) -> bool {
        self.x <= other.x && self.y <= other.y && self.z <= other.z
    }
}

pub trait Point: Copy + Clone + PartialEq + Debug + Sub<Output = Self> {
    type Type: Real;

    fn get_coord_count() -> u32;

    fn get_nth_coord(&self, n: u32) -> Option<Self::Type>;

    fn mean(&self, other: &Self) -> Self;

    fn multiply_coord(&self) -> Self::Type;

    fn has_equal_cords(&self, other: &Self) -> bool;

    fn get_lowest(&self, other: &Self) -> Self;

    fn get_highest(&self, other: &Self) -> Self;

    fn has_higher_cords(&self, other: &Self) -> bool;

    fn has_lower_cords(&self, other: &Self) -> bool;
}

pub trait BoundingBox: Clone + Debug {
    type Point: Point;

    fn get_mbb(&self) -> &Rect<Self::Point>;
    fn get_center(&self) -> Self::Point;
    fn get_coord_count(&self) -> u32 {
        Self::Point::get_coord_count()
    }
    // Area for 2D shapes and volume for 3D.
    fn measure(&self) -> <Self::Point as Point>::Type;
    // Create a minimum bounding box that contains both items.
    fn combine_boxes<B: BoundingBox<Point = Self::Point>>(&self, other: &B) -> Rect<Self::Point>;
    fn is_covering<B: BoundingBox<Point = Self::Point>>(&self, other: &B) -> bool;
    fn is_intersecting<B: BoundingBox<Point = Self::Point>>(&self, other: &B) -> bool;
}

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
