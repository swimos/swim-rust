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

/// Print an inline Recon representation of [`StructuralWritable`] value.
/// TODO Add pretty prining options.
pub fn print_recon<T: StructuralWritable>(value: &T) -> impl Display + '_ {
    ReconPrint(value, StandardPrint)
}

/// Print a compact Recon representation of [`StructuralWritable`] value.
pub fn print_recon_compact<T: StructuralWritable>(value: &T) -> impl Display + '_ {
    ReconPrint(value, CompactPrint)
}

struct ReconPrint<'a, T, S>(&'a T, S);

impl<'a, T: StructuralWritable, S: PrintStrategy + Copy> Display for ReconPrint<'a, T, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ReconPrint(inner, strategy) = self;
        let printer = StructurePrinter::new(f, *strategy);
        inner.write_with(printer)
    }
}

pub struct StructurePrinter<'a, 'b, S> {
    strategy: S,
    fmt: &'a mut Formatter<'b>,
    has_attr: bool,
    brace_written: bool,
    single_item: bool,
    first: bool,
    delegated: bool,
}

impl<'a, 'b, S> StructurePrinter<'a, 'b, S> {
    pub fn new(fmt: &'a mut Formatter<'b>, strategy: S) -> Self {
        StructurePrinter {
            fmt,
            has_attr: false,
            brace_written: false,
            single_item: false,
            first: true,
            delegated: false,
            strategy,
        }
    }

    fn delegate(mut self) -> Self {
        self.delegated = true;
        self
    }
}

struct AttributePrinter<'a, 'b, S> {
    fmt: &'a mut Formatter<'b>,
    has_attr: bool,
    brace_written: bool,
    single_item: bool,
    first: bool,
    delegated: bool,
    strategy: S,
}

impl<'a, 'b, S> AttributePrinter<'a, 'b, S> {
    fn new(fmt: &'a mut Formatter<'b>, strategy: S) -> Self {
        AttributePrinter {
            fmt,
            has_attr: false,
            brace_written: false,
            single_item: false,
            first: true,
            delegated: false,
            strategy,
        }
    }

    fn delegate(mut self) -> Self {
        self.delegated = true;
        self
    }
}

impl<'a, 'b, S> PrimitiveWriter for StructurePrinter<'a, 'b, S>
where
    S: PrintStrategy + Copy,
{
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

impl<'a, 'b, S> StructuralWriter for StructurePrinter<'a, 'b, S>
where
    S: PrintStrategy + Copy,
{
    type Header = Self;
    type Body = Self;

    fn record(self, _num_attrs: usize) -> Result<Self::Header, Self::Error> {
        Ok(self)
    }
}

impl<'a, 'b, S> HeaderWriter for StructurePrinter<'a, 'b, S>
where
    S: PrintStrategy + Copy,
{
    type Repr = ();
    type Error = std::fmt::Error;
    type Body = Self;

    fn write_attr<V: StructuralWritable>(
        mut self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let StructurePrinter {
            fmt,
            has_attr,
            strategy,
            ..
        } = &mut self;
        if *has_attr {
            fmt.write_str(strategy.attr_padding())?;
        } else {
            *has_attr = true;
        }
        write!(fmt, "@{}", name.as_ref())?;
        let attr_printer = AttributePrinter::new(*fmt, *strategy);
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
            strategy,
            ..
        } = &mut self;
        if *has_attr {
            if num_items > 1 {
                fmt.write_str(strategy.attr_padding())?;
                fmt.write_str("{")?;
                fmt.write_str(strategy.block_start_padding(num_items))?;
                *brace_written = true;
            }
        } else {
            fmt.write_str("{")?;
            fmt.write_str(strategy.block_start_padding(num_items))?;
            *brace_written = true;
        }
        *single_item = num_items == 1;
        Ok(self)
    }
}

impl<'a, 'b, S> BodyWriter for StructurePrinter<'a, 'b, S>
where
    S: PrintStrategy + Copy,
{
    type Repr = ();
    type Error = std::fmt::Error;

    fn write_value<V: StructuralWritable>(mut self, value: &V) -> Result<Self, Self::Error> {
        let StructurePrinter {
            fmt,
            brace_written,
            single_item,
            first,
            has_attr,
            strategy,
            ..
        } = &mut self;
        if *has_attr && !*brace_written {
            if *single_item {
                fmt.write_str(" ")?;
            } else {
                fmt.write_str(strategy.attr_padding())?;
                fmt.write_str("{")?;
                *brace_written = true;
            }
        } else if *first {
            *first = false;
        } else {
            fmt.write_str(",")?;
            fmt.write_str(strategy.item_padding())?;
        }
        let printer = StructurePrinter::new(*fmt, *strategy);
        value.write_with(printer)?;
        Ok(self)
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        mut self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let StructurePrinter {
            fmt,
            first,
            strategy,
            ..
        } = &mut self;
        if *first {
            *first = false;
        } else {
            fmt.write_str(",")?;
            fmt.write_str(strategy.item_padding())?;
        }
        let key_printer = StructurePrinter::new(*fmt, *strategy);
        key.write_with(key_printer)?;
        fmt.write_str(":")?;
        fmt.write_str(strategy.slot_padding())?;

        let val_printer = StructurePrinter::new(*fmt, *strategy);
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
            strategy,
            ..
        } = self;
        if brace_written {
            if !first {
                fmt.write_str(strategy.block_end_padding(first))?;
            }
            fmt.write_str("}")?;
        }
        Ok(())
    }
}

fn write_attr_body_val<T: Display>(
    fmt: &mut Formatter<'_>,
    value: &T,
    delegated: bool,
    has_attr: bool,
    strategy: &impl PrintStrategy,
) -> std::fmt::Result {
    if delegated {
        if has_attr {
            write!(fmt, " {}{})", value, strategy.attr_body_padding())
        } else {
            write!(fmt, "{}{})", value, strategy.attr_body_padding())
        }
    } else {
        write!(
            fmt,
            "({}{}{})",
            strategy.attr_body_padding(),
            value,
            strategy.attr_body_padding()
        )
    }
}

impl<'a, 'b, S> PrimitiveWriter for AttributePrinter<'a, 'b, S>
where
    S: PrintStrategy + Copy,
{
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
            strategy,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr, &strategy)
    }

    fn write_i64(self, value: i64) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            strategy,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr, &strategy)
    }

    fn write_u32(self, value: u32) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            strategy,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr, &strategy)
    }

    fn write_u64(self, value: u64) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            strategy,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr, &strategy)
    }

    fn write_f64(self, value: f64) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            strategy,
            ..
        } = self;
        if delegated {
            if has_attr {
                write!(fmt, " {:e}{})", value, strategy.attr_body_padding())
            } else {
                write!(fmt, "{:e}{})", value, strategy.attr_body_padding())
            }
        } else {
            write!(
                fmt,
                "({}{:e}{})",
                strategy.attr_body_padding(),
                value,
                strategy.attr_body_padding()
            )
        }
    }

    fn write_bool(self, value: bool) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            strategy,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr, &strategy)
    }

    fn write_big_int(self, value: BigInt) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            strategy,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr, &strategy)
    }

    fn write_big_uint(self, value: BigUint) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            strategy,
            ..
        } = self;
        write_attr_body_val(fmt, &value, delegated, has_attr, &strategy)
    }

    fn write_text<T: Label>(self, value: T) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            strategy,
            ..
        } = self;
        if delegated {
            if has_attr {
                fmt.write_str(" ")?;
            }
        } else {
            write!(fmt, "({}", strategy.attr_body_padding())?;
        }
        write_string_literal(value.as_ref(), fmt)?;
        write!(fmt, "{})", strategy.attr_body_padding())
    }

    fn write_blob_vec(self, blob: Vec<u8>) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            strategy,
            ..
        } = self;
        let rep = Base64Display::with_config(blob.as_slice(), base64::STANDARD);
        if delegated {
            if has_attr {
                write!(fmt, " %{}{})", &rep, strategy.attr_body_padding())
            } else {
                write!(fmt, "%{}{})", &rep, strategy.attr_body_padding())
            }
        } else {
            write!(
                fmt,
                "({}%{}{})",
                strategy.attr_body_padding(),
                &rep,
                strategy.attr_body_padding()
            )
        }
    }

    fn write_blob(self, value: &[u8]) -> Result<Self::Repr, Self::Error> {
        let AttributePrinter {
            fmt,
            delegated,
            has_attr,
            strategy,
            ..
        } = self;
        let rep = Base64Display::with_config(value, base64::STANDARD);
        if delegated {
            if has_attr {
                write!(fmt, " %{}{})", &rep, strategy.attr_body_padding())
            } else {
                write!(fmt, "%{}{})", &rep, strategy.attr_body_padding())
            }
        } else {
            write!(
                fmt,
                "({}%{}{})",
                strategy.attr_body_padding(),
                &rep,
                strategy.attr_body_padding()
            )
        }
    }
}

impl<'a, 'b, S> StructuralWriter for AttributePrinter<'a, 'b, S>
where
    S: PrintStrategy + Copy,
{
    type Header = Self;
    type Body = Self;

    fn record(mut self, _num_attrs: usize) -> Result<Self::Header, Self::Error> {
        let AttributePrinter { fmt, strategy, .. } = &mut self;
        write!(fmt, "({}", strategy.attr_body_padding())?;
        Ok(self)
    }
}

impl<'a, 'b, S> HeaderWriter for AttributePrinter<'a, 'b, S>
where
    S: PrintStrategy + Copy,
{
    type Repr = ();
    type Error = std::fmt::Error;
    type Body = Self;

    fn write_attr<V: StructuralWritable>(
        mut self,
        name: Cow<'_, str>,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let AttributePrinter {
            fmt,
            has_attr,
            strategy,
            ..
        } = &mut self;
        if *has_attr {
            fmt.write_str(strategy.attr_padding())?;
        } else {
            *has_attr = true;
        }
        write!(fmt, "@{}", name.as_ref())?;
        let attr_printer = AttributePrinter::new(*fmt, *strategy);
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
            strategy,
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
                    fmt.write_str(strategy.attr_padding())?;
                    fmt.write_str("{")?;
                    fmt.write_str(strategy.block_start_padding(num_items))?;
                    *brace_written = true;
                }
            }
        } else if num_items == 0 {
            fmt.write_str("{")?;
            fmt.write_str(strategy.block_start_padding(num_items))?;
            *brace_written = true;
        }
        Ok(self)
    }
}

impl<'a, 'b, S> BodyWriter for AttributePrinter<'a, 'b, S>
where
    S: PrintStrategy + Copy,
{
    type Repr = ();
    type Error = std::fmt::Error;

    fn write_value<V: StructuralWritable>(mut self, value: &V) -> Result<Self, Self::Error> {
        let AttributePrinter {
            fmt,
            brace_written,
            single_item,
            first,
            has_attr,
            strategy,
            ..
        } = &mut self;
        if !*brace_written && !*has_attr && *single_item {
            fmt.write_str("{")?;
            fmt.write_str(strategy.block_start_padding(1))?;
            *brace_written = true;
        }
        if *first {
            *first = false;
        } else {
            fmt.write_str(",")?;
            fmt.write_str(strategy.item_padding())?;
        }
        let printer = StructurePrinter::new(*fmt, *strategy);
        value.write_with(printer)?;
        Ok(self)
    }

    fn write_slot<K: StructuralWritable, V: StructuralWritable>(
        mut self,
        key: &K,
        value: &V,
    ) -> Result<Self, Self::Error> {
        let AttributePrinter {
            fmt,
            first,
            strategy,
            ..
        } = &mut self;
        if *first {
            *first = false;
        } else {
            fmt.write_str(",")?;
            fmt.write_str(strategy.item_padding())?;
        }
        let key_printer = StructurePrinter::new(*fmt, *strategy);
        key.write_with(key_printer)?;
        fmt.write_str(":")?;
        fmt.write_str(strategy.slot_padding())?;

        let val_printer = StructurePrinter::new(*fmt, *strategy);
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

pub trait PrintStrategy {
    fn attr_padding(&self) -> &'static str;

    fn attr_body_padding(&self) -> &'static str;

    fn block_start_padding(&self, items: usize) -> &'static str;

    fn block_end_padding(&self, is_empty: bool) -> &'static str;

    fn item_padding(&self) -> &'static str;

    fn slot_padding(&self) -> &'static str;
}

#[derive(Clone, Copy)]
struct StandardPrint;

#[derive(Clone, Copy)]
struct CompactPrint;

impl PrintStrategy for StandardPrint {
    fn attr_padding(&self) -> &'static str {
        " "
    }

    fn attr_body_padding(&self) -> &'static str {
        ""
    }

    fn block_start_padding(&self, items: usize) -> &'static str {
        if items == 0 {
            ""
        } else {
            " "
        }
    }

    fn block_end_padding(&self, is_empty: bool) -> &'static str {
        if is_empty {
            ""
        } else {
            " "
        }
    }

    fn item_padding(&self) -> &'static str {
        " "
    }

    fn slot_padding(&self) -> &'static str {
        " "
    }
}

impl PrintStrategy for CompactPrint {
    fn attr_padding(&self) -> &'static str {
        ""
    }

    fn attr_body_padding(&self) -> &'static str {
        ""
    }

    fn block_start_padding(&self, _items: usize) -> &'static str {
        ""
    }

    fn block_end_padding(&self, _is_empty: bool) -> &'static str {
        ""
    }

    fn item_padding(&self) -> &'static str {
        ""
    }

    fn slot_padding(&self) -> &'static str {
        ""
    }
}
