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

mod enumeration;
// mod impls;
// mod structure;

// fn a {
// enum Example {
//     A {
//         a: String,
//         b: Option<i64>,
//     },
// }
//
// impl crate::form::Form for Example
// {
//     #[inline]
//     #[allow(non_snake_case)]
//     fn as_value(&self) -> crate::model
//     ::Value
//     {
//         match *self
//         {
//             Example::A { ref a, ref b, } =>
//                 {
//                     let mut attrs = vec![crate::model::Attr::
//                                          of(("A", crate::model::Value::
//                     Record(Vec::new(), vec![crate::model::Item::
//                                             Slot(crate::model::Value::
//                                                  Text("b".to_string()), b.
//                         as_value()), ]))), ];
//                     crate::model::Value
//                     ::
//                     Record(attrs, vec![crate::model::Item::
//                                        Slot(crate::model::Value::
//                                             Text("a".to_string()), a.as_value()), ])
//                 }
//         }
//     }
//     #[inline]
//     #[allow(non_snake_case)]
//     fn
//     try_from_value(value: &crate::model::Value) -> Result<Self, crate
//     ::form::FormErr>
//     {
//         match value
//         {
//             crate::model::Value::Record(attrs, items) => match attrs.
//                 first()
//             {
//                 Some(crate::model::Attr { name, value }) if name.
//                     as_ref() == "A" =>
//                     {
//                         let mut __opt_a: std::option::Option<String> =
//                             None;
//                         let mut __opt_b: std::option::Option<Option
//                         <i64>> = None;
//                         let mut attr_it = attrs.iter();
//                         while let Some(Attr { name, ref value }) = attr_it.
//                             next()
//                         {
//                             match name.as_ref()
//                             {
//                                 "A" => match value
//                                 {
//                                     crate::model::Value::
//                                     Record(_attrs, items) =>
//                                         {
//                                             let mut iter_items = items.iter();
//                                             while let Some(item) = iter_items.next()
//                                             {
//                                                 match item
//                                                 {
//                                                     crate::model::Item::
//                                                     Slot(crate::model::Value::
//                                                          Text(name), ref v) if name ==
//                                                         "b" =>
//                                                         {
//                                                             __opt_b = std::option::
//                                                             Option::
//                                                             Some(crate::form::Form::
//                                                             try_from_value(v)?);
//                                                         }
//                                                     i => return
//                                                         Err(crate::form::FormErr::
//                                                         Message(format!("Unexpected item: {:?}",
//                                                                         i))),
//                                                 }
//                                             }
//                                         }
//                                     crate::model::Value::Extant => {}
//                                     _
//                                     => return
//                                         Err(crate::form::FormErr::Malformatted),
//                                 },
//                                 _ => return
//                                     Err(crate::form::FormErr::MismatchedTag),
//                             }
//                         }
//                         let mut items_iter = items.iter();
//                         while let
//                         Some(item) = items_iter.next()
//                         {
//                             match item
//                             {
//                                 crate::model::Item::
//                                 Slot(crate::model::Value::Text(name), v) if
//                                 name == "a" =>
//                                     {
//                                         if __opt_a.is_some()
//                                         {
//                                             return
//                                                 Err(crate::form::FormErr::
//                                                 DuplicateField(String::from("a")));
//                                         } else {
//                                             __opt_a = std::option::Option::
//                                             Some(crate::form::Form::
//                                             try_from_value(v)?);
//                                         }
//                                     }
//                                 i => return
//                                     Err(crate::form::FormErr::
//                                     Message(format!("Unexpected item: {:?}", i))),
//                             }
//                         }
//                         Ok(Example::A
//                         {
//                             a: __opt_a.
//                                 ok_or(crate::form::FormErr::
//                                 Message(String::
//                                 from("Missing field: __opt_a")))?,
//                             b
//                             : __opt_b.
//                                 ok_or(crate::form::FormErr::
//                                 Message(String::
//                                 from("Missing field: __opt_b")))?,
//                         })
//                     }
//             }
//             _ => return
//                 Err(crate::form::FormErr::
//                 Message(String::from("Expected record"))),
//         }
//     }
// }
// }
