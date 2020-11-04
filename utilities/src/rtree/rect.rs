use num::Signed;
use std::marker::PhantomData;

#[derive(Debug, Clone, PartialEq)]
pub struct Rect<T, P>
where
    T: PartialOrd + Copy + Clone + Signed,
    P: Point<T>,
{
    low: P,
    high: P,
    phantom: PhantomData<T>,
}

impl<T, P> Rect<T, P>
where
    T: PartialOrd + Copy + Clone + Signed,
    P: Point<T>,
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

impl<T, P> BoundingBox<T, P> for Rect<T, P>
where
    T: PartialOrd + Copy + Clone + Signed,
    P: Point<T>,
{
    fn get_mbb(&self) -> &Rect<T, P> {
        self
    }

    fn measure(&self) -> T {
        self.high.diff(&self.low).multiply_coord()
    }

    fn combine_boxes<B: BoundingBox<T, P>>(&self, other: &B) -> Rect<T, P> {
        let other_mbb = other.get_mbb();

        let new_low = self.low.get_lowest(&other_mbb.low);
        let new_high = self.high.get_highest(&other_mbb.high);

        Rect::new(new_low, new_high)
    }

    fn is_covering<B: BoundingBox<T, P>>(&self, other: &B) -> bool {
        let other_mbb = other.get_mbb();
        self.low.has_lower_cords(&other_mbb.low) && self.high.has_higher_cords(&other_mbb.high)
    }

    fn is_intersecting<B: BoundingBox<T, P>>(&self, other: &B) -> bool {
        let other_mbb = other.get_mbb();
        !self.low.has_higher_cords(&other_mbb.high) && !self.high.has_lower_cords(&other_mbb.low)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Point2D<T: PartialOrd + Clone> {
    //Todo remove pub
    pub x: T,
    pub y: T,
}

impl<T: PartialOrd + Clone> Point2D<T> {
    pub fn new(x: T, y: T) -> Self {
        Point2D { x, y }
    }
}

impl<T: PartialOrd + Copy + Clone + Signed> Point<T> for Point2D<T> {
    fn sum(&self, other: &Point2D<T>) -> Point2D<T> {
        Point2D {
            x: self.x + other.x,
            y: self.y + other.y,
        }
    }

    fn diff(&self, other: &Point2D<T>) -> Point2D<T> {
        Point2D {
            x: self.x - other.x,
            y: self.y - other.y,
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

#[derive(Debug, Clone, PartialEq)]
pub struct Point3D<T: PartialOrd + Clone> {
    x: T,
    y: T,
    z: T,
}

impl<T: PartialOrd + Clone> Point3D<T> {
    pub fn new(x: T, y: T, z: T) -> Self {
        Point3D { x, y, z }
    }
}

impl<T: PartialOrd + Copy + Clone + Signed> Point<T> for Point3D<T> {
    fn sum(&self, other: &Point3D<T>) -> Point3D<T> {
        Point3D {
            x: self.x + other.x,
            y: self.y + other.y,
            z: self.z + other.z,
        }
    }

    fn diff(&self, other: &Point3D<T>) -> Point3D<T> {
        Point3D {
            x: self.x - other.x,
            y: self.y - other.y,
            z: self.z - other.z,
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

pub trait Point<T>: Clone + PartialEq
where
    T: PartialOrd + Copy + Clone + Signed,
{
    fn sum(&self, other: &Self) -> Self;

    fn diff(&self, other: &Self) -> Self;

    fn multiply_coord(&self) -> T;

    fn has_equal_cords(&self, other: &Self) -> bool;

    fn get_lowest(&self, other: &Self) -> Self;

    fn get_highest(&self, other: &Self) -> Self;

    fn has_higher_cords(&self, other: &Self) -> bool;

    fn has_lower_cords(&self, other: &Self) -> bool;
}

pub trait BoundingBox<T, P>: Clone
where
    T: PartialOrd + Copy + Clone + Signed,
    P: Point<T>,
{
    fn get_mbb(&self) -> &Rect<T, P>;
    // Area for 2D shapes and volume for 3D.
    fn measure(&self) -> T;
    // Create a minimum bounding box that contains both items.
    fn combine_boxes<B: BoundingBox<T, P>>(&self, other: &B) -> Rect<T, P>;
    fn is_covering<B: BoundingBox<T, P>>(&self, other: &B) -> bool;
    fn is_intersecting<B: BoundingBox<T, P>>(&self, other: &B) -> bool;
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
