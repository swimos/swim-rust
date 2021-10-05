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

#[cfg(test)]
mod tests;

use base64::display::Base64Display;
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use swim_form::structural::write::{
    BodyWriter, HeaderWriter, Label, PrimitiveWriter, RecordBodyKind, StructuralWritable,
    StructuralWriter,
};
use swim_model::bigint::{BigInt, BigUint};
use swim_model::write_string_literal;

/// Print a compact Recon representation of [`StructuralWritable`] value.
/// TODO Add pretty prining options.
pub fn print_recon<T: StructuralWritable>(value: &T) -> impl Display + '_ {
    ReconPrint(value)
}

struct ReconPrint<'a, T>(&'a T);

impl<'a, T: StructuralWritable> Display for ReconPrint<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ReconPrint(inner) = self;
        let printer = StructurePrinter::new(f);
        inner.write_with(printer)
    }
}

pub struct StructurePrinter<'a, 'b> {
    fmt: &'a mut Formatter<'b>,
    has_attr: bool,
    brace_written: bool,
    single_item: bool,
    first: bool,
    delegated: bool,
}

impl<'a, 'b> StructurePrinter<'a, 'b> {
    pub fn new(fmt: &'a mut Formatter<'b>) -> Self {
        StructurePrinter {
            fmt,
            has_attr: false,
            brace_written: false,
            single_item: false,
            first: true,
            delegated: false,
        }
    }

    fn delegate(mut self) -> Self {
        self.delegated = true;
        self
    }
}

struct AttributePrinter<'a, 'b> {
    fmt: &'a mut Formatter<'b>,
    has_attr: bool,
    brace_written: bool,
    single_item: bool,
    first: bool,
    delegated: bool,
}

impl<'a, 'b> AttributePrinter<'a, 'b> {
    fn new(fmt: &'a mut Formatter<'b>) -> Self {
        AttributePrinter {
            fmt,
            has_attr: false,
            brace_written: false,
            single_item: false,
            first: true,
            delegated: false,
        }
    }

    fn delegate(mut self) -> Self {
        self.delegated = true;
        self
    }
}

impl<'a, 'b> PrimitiveWriter for StructurePrinter<'a, 'b> {
    type Repr = ();
    type Error = std::fmt::Error;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        Ok(())
    }

    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error> {
        let StructurePrinter { fmt, has_attr, .. } = self;
        if has_attr {
            write!(fmt, " {}", value)
        } else {
            write!(fmt, "{}", value)
        }
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        let StructurePrinter { fmt, has_attr, .. } = self;
        if has_attr {
            write!(fmt, " {}", value)
        } else {
            write!(fmt, "{}", value)
        }
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        let StructurePrinter { fmt, has_attr, .. } = self;
        if has_attr {
            write!(fmt, " {}", value)
        } else {
            write!(fmt, "{}", value)
        }
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        let StructurePrinter { fmt, has_attr, .. } = self;
        if has_attr {
            write!(fmt, " {}", value)
        } else {
            write!(fmt, "{}", value)
        }
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        let StructurePrinter { fmt, has_attr, .. } = self;
        let mut buffer = ryu::Buffer::new();
        let float_string = buffer.format(value);
        if has_attr {
            write!(fmt, " {}", float_string)
        } else {
            write!(fmt, "{}", float_string)
        }
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        let StructurePrinter { fmt, has_attr, .. } = self;
        if has_attr {
            write!(fmt, " {}", value)
        } else {
            write!(fmt, "{}", value)
        }
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        let StructurePrinter { fmt, has_attr, .. } = self;
        if has_attr {
            write!(fmt, " {}", value)
        } else {
            write!(fmt, "{}", value)
        }
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        let StructurePrinter { fmt, has_attr, .. } = self;
        if has_attr {
            write!(fmt, " {}", value)
        } else {
            write!(fmt, "{}", value)
        }
    }

    fn write_text<T: Label>(self, value: T) -> Result<Self::Repr, Self::Error> {
        let StructurePrinter { fmt, has_attr, .. } = self;
        if has_attr {
            fmt.write_str(" ")?;
        }
        write_string_literal(value.as_ref(), fmt)
    }

    fn write_blob_vec(self, blob: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        let StructurePrinter { fmt, has_attr, .. } = self;
        if has_attr {
            write!(
                fmt,
                " %{}",
                Base64Display::with_config(blob.as_slice(), base64::STANDARD)
            )
        } else {
            write!(
                fmt,
                "%{}",
                Base64Display::with_config(blob.as_slice(), base64::STANDARD)
            )
        }
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        let StructurePrinter { fmt, has_attr, .. } = self;
        if has_attr {
            write!(
                fmt,
                " %{}",
                Base64Display::with_config(value, base64::STANDARD)
            )
        } else {
            write!(
                fmt,
                "%{}",
                Base64Display::with_config(value, base64::STANDARD)
            )
        }
    }
}

impl<'a, 'b> StructuralWriter for StructurePrinter<'a, 'b> {
    type Header = Self;
    type Body = Self;

    fn record(self, _num_attrs: usize) -> Result<Self::Header, Self::Error> {
        Ok(self)
    }
}

impl<'a, 'b> HeaderWriter for StructurePrinter<'a, 'b> {
    type Repr = ();
    type Error = std::fmt::Error;
    type Body = Self;

    fn write_attr<V: StructuralWritable>(
        mut self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let StructurePrinter { fmt, has_attr, .. } = &mut self;
        if *has_attr {
            fmt.write_str(" ")?;
        } else {
            *has_attr = true;
        }
        write!(fmt, "@{}", name.as_ref())?;
        let attr_printer = AttributePrinter::new(*fmt);
        value.write_with(attr_printer)?;
        Ok(self)
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        value.write_with(self.delegate())
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(
        self,
        name: L,
        value: V,
    ) -> Result<Self, Self::Error> {
        self.write_attr(Cow::Borrowed(name.as_ref()), &value)
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        value.write_with(self.delegate())
    }

    fn complete_header(
        mut self,
        _kind: RecordBodyKind,
        num_items: usize,
    ) -> Result<Self::Body, Self::Error> {
        let StructurePrinter {
            fmt,
            has_attr,
            brace_written,
            single_item,
            ..
        } = &mut self;
        *single_item = num_items == 1;
        if *has_attr {
            if num_items > 1 {
                fmt.write_str(" { ")?;
                *brace_written = true;
            }
        } else {
            if num_items == 0 {
                fmt.write_str("{")?;
            } else {
                fmt.write_str("{ ")?;
            }
            *brace_written = true;
        }
        Ok(self)
    }
}

impl<'a, 'b> BodyWriter for StructurePrinter<'a, 'b> {
    type Repr = ();
    type Error = std::fmt::Error;

    fn write_value<V: StructuralWritable>(mut self, value: &V) -> Result<Self, Self::Error> {
        let StructurePrinter {
            fmt,
            brace_written,
            single_item,
            first,
            has_attr,
            ..
        } = &mut self;
        if *has_attr && !*brace_written {
            if *single_item {
                fmt.write_str(" ")?;
            } else {
                fmt.write_str(" {")?;
                *brace_written = true;
            }
        } else if *first {
            *first = false;
        } else {
            fmt.write_str(", ")?;
        }
        let printer = StructurePrinter::new(*fmt);
        value.write_with(printer)?;
        Ok(self)
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        mut self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let StructurePrinter { fmt, first, .. } = &mut self;
        if *first {
            *first = false;
        } else {
            fmt.write_str(", ")?;
        }
        let key_printer = StructurePrinter::new(*fmt);
        key.write_with(key_printer)?;
        fmt.write_str(": ")?;

        let val_printer = StructurePrinter::new(*fmt);
        value.write_with(val_printer)?;
        Ok(self)
    }

    fn write_value_into<V: StructuralWritable>(self, value: V) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: K,
        value: V,
    ) -> Result<Self, Self::Error> {
        self.write_slot(&key, &value)
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        let StructurePrinter {
            fmt,
            brace_written,
            first,
            ..
        } = self;
        if brace_written {
            if first {
                fmt.write_str("}")?;
            } else {
                fmt.write_str(" }")?;
            }
        }
        Ok(())
    }
}

fn write_attr_body_val<T: Display>(
    fmt: &mut Formatter<'_>,
    value: &T,
    delegated: bool,
    has_attr: bool,
) -> std::fmt::Result {
    if delegated {
        if has_attr {
            write!(fmt, " {})", value)
        } else {
            write!(fmt, "{})", value)
        }
    } else {
        write!(fmt, "({})", value)
    }
}

impl<'a, 'b> PrimitiveWriter for AttributePrinter<'a, 'b> {
    type Repr = ();
    type Error = std::fmt::Error;

    fn write_extant(self) -> Result<Self::Repr, Self::Error> {
        Ok(())
    }

    fn write_i32(self, value: i32) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr)
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr)
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr)
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr)
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            ..
        } = self;
        if delegated {
            if has_attr {
                write!(fmt, " {:e})", value)
            } else {
                write!(fmt, "{:e})", value)
            }
        } else {
            write!(fmt, "({:e})", value)
        }
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr)
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr)
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr)
    }

    fn write_text<T: Label>(self, value: T) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            ..
        } = self;
        if delegated {
            if has_attr {
                fmt.write_str(" ")?;
            }
            write_string_literal(value.as_ref(), fmt)
        } else {
            fmt.write_str("(")?;
            write_string_literal(value.as_ref(), fmt)?;
            fmt.write_str(")")
        }
    }

    fn write_blob_vec(self, blob: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            ..
        } = self;
        let rep = Base64Display::with_config(blob.as_slice(), base64::STANDARD);
        if delegated {
            if has_attr {
                write!(fmt, " %{})", &rep)
            } else {
                write!(fmt, "%{})", &rep)
            }
        } else {
            write!(fmt, "(%{})", &rep)
        }
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            ..
        } = self;
        let rep = Base64Display::with_config(value, base64::STANDARD);
        if delegated {
            if has_attr {
                write!(fmt, " %{})", &rep)
            } else {
                write!(fmt, "%{})", &rep)
            }
        } else {
            write!(fmt, "(%{})", &rep)
        }
    }
}

impl<'a, 'b> StructuralWriter for AttributePrinter<'a, 'b> {
    type Header = Self;
    type Body = Self;

    fn record(mut self, _num_attrs: usize) -> Result<Self::Header, Self::Error> {
        let AttributePrinter { fmt, .. } = &mut self;
        fmt.write_str("(")?;
        Ok(self)
    }
}

impl<'a, 'b> HeaderWriter for AttributePrinter<'a, 'b> {
    type Repr = ();
    type Error = std::fmt::Error;
    type Body = Self;

    fn write_attr<V: StructuralWritable>(
        mut self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let AttributePrinter { fmt, has_attr, .. } = &mut self;
        if *has_attr {
            fmt.write_str(" ")?;
        } else {
            *has_attr = true;
        }
        write!(fmt, "@{}", name.as_ref())?;
        let attr_printer = AttributePrinter::new(*fmt);
        value.write_with(attr_printer)?;
        Ok(self)
    }

    fn delegate<V: StructuralWritable>(self, value: &V) -> Result<Self::Repr, Self::Error> {
        value.write_with(self.delegate())
    }

    fn write_attr_into<L: Label, V: StructuralWritable>(
        self,
        name: L,
        value: V,
    ) -> Result<Self, Self::Error> {
        self.write_attr(Cow::Borrowed(name.as_ref()), &value)
    }

    fn delegate_into<V: StructuralWritable>(self, value: V) -> Result<Self::Repr, Self::Error> {
        value.write_with(self.delegate())
    }

    fn complete_header(
        mut self,
        _kind: RecordBodyKind,
        num_items: usize,
    ) -> Result<Self::Body, Self::Error> {
        let AttributePrinter {
            fmt,
            has_attr,
            brace_written,
            single_item,
            ..
        } = &mut self;
        *single_item = num_items == 1;
        if *has_attr {
            match num_items {
                0 => {}
                1 => {
                    fmt.write_str(" ")?;
                }
                _ => {
                    fmt.write_str(" { ")?;
                    *brace_written = true;
                }
            }
        } else if num_items == 0 {
            fmt.write_str("{")?;
            *brace_written = true;
        }
        Ok(self)
    }
}

impl<'a, 'b> BodyWriter for AttributePrinter<'a, 'b> {
    type Repr = ();
    type Error = std::fmt::Error;

    fn write_value<V: StructuralWritable>(mut self, value: &V) -> Result<Self, Self::Error> {
        let AttributePrinter {
            fmt,
            brace_written,
            single_item,
            first,
            has_attr,
            ..
        } = &mut self;
        if !*brace_written && !*has_attr && *single_item {
            fmt.write_str("{ ")?;
            *brace_written = true;
        }
        if *first {
            *first = false;
        } else {
            fmt.write_str(", ")?;
        }
        let printer = StructurePrinter::new(*fmt);
        value.write_with(printer)?;
        Ok(self)
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        mut self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let AttributePrinter { fmt, first, .. } = &mut self;
        if *first {
            *first = false;
        } else {
            fmt.write_str(", ")?;
        }
        let key_printer = StructurePrinter::new(*fmt);
        key.write_with(key_printer)?;
        fmt.write_str(": ")?;

        let val_printer = StructurePrinter::new(*fmt);
        value.write_with(val_printer)?;
        Ok(self)
    }

    fn write_value_into<V: StructuralWritable>(self, value: V) -> Result<Self, Self::Error> {
        self.write_value(&value)
    }

    fn write_slot_into<K: StructuralWritable, V: StructuralWritable>(
        self,
        key: K,
        value: V,
    ) -> Result<Self, Self::Error> {
        self.write_slot(&key, &value)
    }

    fn done(self) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            brace_written,
            first,
            ..
        } = self;
        if brace_written {
            if first {
                fmt.write_str("}")?;
            } else {
                fmt.write_str(" }")?;
            }
        }
        fmt.write_str(")")?;
        Ok(())
    }
}
