use num::{Signed, ToPrimitive};
use std::marker::PhantomData;

#[derive(Debug, Clone, PartialEq)]
pub struct Rect<C, P>
where
    C: Coordinate,
    P: Point<C>,
{
    low: P,
    high: P,
    phantom: PhantomData<C>,
}

impl<C, P> Rect<C, P>
where
    C: Coordinate,
    P: Point<C>,
{
    pub fn new(low: P, high: P) -> Self {
        if low.has_higher_cords(&high) || low.has_equal_cords(&high) {
            panic!("The first point must be lower than the second.")
        }

        Rect {
            low,
            high,
            phantom: PhantomData,
        }
    }

    pub fn get_low(&self) -> &P {
        &self.low
    }

    pub fn get_high(&self) -> &P {
        &self.high
    }
}

impl<C, P> BoundingBox<C, P> for Rect<C, P>
where
    C: Coordinate,
    P: Point<C>,
{
    fn get_mbb(&self) -> &Rect<C, P> {
        self
    }

    fn get_center(&self) -> P::Mean {
        self.get_high().mean(self.get_low())
    }

    fn measure(&self) -> C {
        self.high.diff(&self.low).multiply_coord()
    }

    fn combine_boxes<B: BoundingBox<C, P>>(&self, other: &B) -> Rect<C, P> {
        let other_mbb = other.get_mbb();

        let new_low = self.low.get_lowest(&other_mbb.low);
        let new_high = self.high.get_highest(&other_mbb.high);

        Rect::new(new_low, new_high)
    }

    fn is_covering<B: BoundingBox<C, P>>(&self, other: &B) -> bool {
        let other_mbb = other.get_mbb();
        self.low.has_lower_cords(&other_mbb.low) && self.high.has_higher_cords(&other_mbb.high)
    }

    fn is_intersecting<B: BoundingBox<C, P>>(&self, other: &B) -> bool {
        let other_mbb = other.get_mbb();
        !self.low.has_higher_cords(&other_mbb.high) && !self.high.has_lower_cords(&other_mbb.low)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Point2D<C: Coordinate> {
    x: C,
    y: C,
}

impl<C: Coordinate> Point2D<C> {
    pub fn new(x: C, y: C) -> Self {
        Point2D { x, y }
    }
}

impl<C: Coordinate> Point<C> for Point2D<C> {
    type Mean = Point2D<f64>;

    fn get_coord_count() -> u32 {
        2
    }

    fn get_nth_coord(&self, n: usize) -> Option<C> {
        if n == 0 {
            Some(self.x)
        } else if n == 1 {
            Some(self.y)
        } else {
            None
        }
    }

    fn sum(&self, other: &Point2D<C>) -> Point2D<C> {
        Point2D {
            x: self.x + other.x,
            y: self.y + other.y,
        }
    }

    fn diff(&self, other: &Point2D<C>) -> Point2D<C> {
        Point2D {
            x: self.x - other.x,
            y: self.y - other.y,
        }
    }

    fn mean(&self, other: &Point2D<C>) -> Self::Mean {
        Point2D {
            x: (self.x + other.x).to_f64().unwrap() / 2.0,
            y: (self.y + other.y).to_f64().unwrap() / 2.0,
        }
    }

    fn multiply_coord(&self) -> C {
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

#[derive(Debug, Clone, PartialEq)]
pub struct Point3D<C: Coordinate> {
    x: C,
    y: C,
    z: C,
}

impl<C: Coordinate> Point3D<C> {
    pub fn new(x: C, y: C, z: C) -> Self {
        Point3D { x, y, z }
    }
}

impl<C: Coordinate> Point<C> for Point3D<C> {
    type Mean = Point3D<f64>;

    fn get_coord_count() -> u32 {
        3
    }

    fn get_nth_coord(&self, n: usize) -> Option<C> {
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

    fn sum(&self, other: &Point3D<C>) -> Point3D<C> {
        Point3D {
            x: self.x + other.x,
            y: self.y + other.y,
            z: self.z + other.z,
        }
    }

    fn diff(&self, other: &Point3D<C>) -> Point3D<C> {
        Point3D {
            x: self.x - other.x,
            y: self.y - other.y,
            z: self.z - other.z,
        }
    }

    fn mean(&self, other: &Self) -> Self::Mean {
        Point3D {
            x: (self.x + other.x).to_f64().unwrap() / 2.0,
            y: (self.y + other.y).to_f64().unwrap() / 2.0,
            z: (self.z + other.z).to_f64().unwrap() / 2.0,
        }
    }

    fn multiply_coord(&self) -> C {
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

pub trait Coordinate: PartialOrd + Copy + Signed + ToPrimitive {}

impl<C: PartialOrd + Copy + Signed + ToPrimitive> Coordinate for C {}

pub trait Point<C>: Clone + PartialEq
where
    C: Coordinate,
{
    type Mean: Point<f64>;

    fn get_coord_count() -> u32;

    fn get_nth_coord(&self, n: usize) -> Option<C>;

    fn sum(&self, other: &Self) -> Self;

    fn diff(&self, other: &Self) -> Self;

    fn mean(&self, other: &Self) -> Self::Mean;

    fn multiply_coord(&self) -> C;

    fn has_equal_cords(&self, other: &Self) -> bool;

    fn get_lowest(&self, other: &Self) -> Self;

    fn get_highest(&self, other: &Self) -> Self;

    fn has_higher_cords(&self, other: &Self) -> bool;

    fn has_lower_cords(&self, other: &Self) -> bool;
}

pub trait BoundingBox<C, P>: Clone
where
    C: Coordinate,
    P: Point<C>,
{
    fn get_mbb(&self) -> &Rect<C, P>;
    fn get_center(&self) -> P::Mean;
    // Area for 2D shapes and volume for 3D.
    fn measure(&self) -> C;
    // Create a minimum bounding box that contains both items.
    fn combine_boxes<B: BoundingBox<C, P>>(&self, other: &B) -> Rect<C, P>;
    fn is_covering<B: BoundingBox<C, P>>(&self, other: &B) -> bool;
    fn is_intersecting<B: BoundingBox<C, P>>(&self, other: &B) -> bool;
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
