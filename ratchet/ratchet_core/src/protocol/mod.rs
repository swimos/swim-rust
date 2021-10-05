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

mod frame;
mod mask;

pub use frame::*;
pub use mask::apply_mask;

use derive_more::Display;
use std::convert::TryFrom;
use thiserror::Error;

bitflags::bitflags! {
    pub struct HeaderFlags: u8 {
        const FIN       = 0b1000_0000;

        const RSV_1     = 0b0100_0000;
        const RSV_2     = 0b0010_0000;
        const RSV_3     = 0b0001_0000;

        // The extension bits that *may* be high. Anything outside this range is illegal.
        const RESERVED  = Self::RSV_1.bits | Self::RSV_2.bits | Self::RSV_3.bits;

        // no new flags should be added
    }
}

#[allow(warnings)]
impl HeaderFlags {
    pub fn is_fin(&self) -> bool {
        self.contains(HeaderFlags::FIN)
    }

    pub fn is_rsv1(&self) -> bool {
        self.contains(HeaderFlags::RSV_1)
    }

    pub fn is_rsv2(&self) -> bool {
        self.contains(HeaderFlags::RSV_2)
    }

    pub fn is_rsv3(&self) -> bool {
        self.contains(HeaderFlags::RSV_3)
    }
}

#[derive(Debug)]
pub enum Message {
    Text,
    Binary,
    Ping,
    Pong,
    Close(Option<CloseReason>),
}

impl Message {
    pub fn is_text(&self) -> bool {
        matches!(self, Message::Text)
    }

    pub fn is_binary(&self) -> bool {
        matches!(self, Message::Binary)
    }

    pub fn is_ping(&self) -> bool {
        matches!(self, Message::Ping)
    }

    pub fn is_pong(&self) -> bool {
        matches!(self, Message::Pong)
    }

    pub fn is_close(&self) -> bool {
        matches!(self, Message::Close(_))
    }
}

#[derive(Debug)]
pub enum PayloadType {
    Text,
    Binary,
    Ping,
}

#[derive(Debug)]
pub enum MessageType {
    Text,
    Binary,
}

#[derive(Clone)]
pub struct WebSocketConfig {
    pub max_size: usize,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        WebSocketConfig {
            max_size: usize::MAX,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Role {
    Client,
    Server,
}

impl Role {
    pub fn is_server(&self) -> bool {
        matches!(self, Role::Server)
    }
}

#[derive(Debug, Copy, Clone, Display, PartialEq)]
pub enum OpCode {
    #[display(fmt = "{}", _0)]
    DataCode(DataCode),
    #[display(fmt = "{}", _0)]
    ControlCode(ControlCode),
}

impl OpCode {
    pub fn is_data(&self) -> bool {
        matches!(self, OpCode::DataCode(_))
    }

    pub fn is_control(&self) -> bool {
        matches!(self, OpCode::ControlCode(_))
    }
}

impl From<OpCode> for u8 {
    fn from(op: OpCode) -> Self {
        match op {
            OpCode::DataCode(code) => code as u8,
            OpCode::ControlCode(code) => code as u8,
        }
    }
}

#[derive(Debug, Copy, Clone, Display, PartialEq)]
pub enum DataCode {
    Continuation = 0,
    Text = 1,
    Binary = 2,
}

impl From<DataCode> for ratchet_ext::OpCode {
    fn from(e: DataCode) -> Self {
        match e {
            DataCode::Continuation => ratchet_ext::OpCode::Continuation,
            DataCode::Text => ratchet_ext::OpCode::Text,
            DataCode::Binary => ratchet_ext::OpCode::Binary,
        }
    }
}

#[derive(Debug, Copy, Clone, Display, PartialEq)]
pub enum ControlCode {
    Close = 8,
    Ping = 9,
    Pong = 10,
}

#[derive(Debug, Error, PartialEq)]
pub enum OpCodeParseErr {
    #[error("Reserved OpCode: `{0}`")]
    Reserved(u8),
    #[error("Invalid OpCode: `{0}`")]
    Invalid(u8),
}

impl TryFrom<u8> for OpCode {
    type Error = OpCodeParseErr;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(OpCode::DataCode(DataCode::Continuation)),
            1 => Ok(OpCode::DataCode(DataCode::Text)),
            2 => Ok(OpCode::DataCode(DataCode::Binary)),
            r @ 3..=7 => Err(OpCodeParseErr::Reserved(r)),
            8 => Ok(OpCode::ControlCode(ControlCode::Close)),
            9 => Ok(OpCode::ControlCode(ControlCode::Ping)),
            10 => Ok(OpCode::ControlCode(ControlCode::Pong)),
            r @ 11..=15 => Err(OpCodeParseErr::Reserved(r)),
            e => Err(OpCodeParseErr::Invalid(e)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CloseReason {
    pub code: CloseCode,
    pub description: Option<String>,
}

impl CloseReason {
    pub fn new(code: CloseCode, description: Option<String>) -> Self {
        CloseReason { code, description }
    }
}

/// # Additional implementation sources:
/// https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent
/// https://mailarchive.ietf.org/arch/msg/hybi/P_1vbD9uyHl63nbIIbFxKMfSwcM/
/// https://tools.ietf.org/id/draft-ietf-hybi-thewebsocketprotocol-09.html
#[derive(Clone, Debug, PartialEq)]
pub enum CloseCode {
    Normal,
    GoingAway,
    Protocol,
    Unsupported,
    Status,
    Abnormal,
    Invalid,
    Policy,
    Overflow,
    Extension,
    Unexpected,
    Restarting,
    TryAgain,
    Tls,
    ReservedExtension(u16),
    Library(u16),
    Application(u16),
}

impl CloseCode {
    pub fn is_illegal(&self) -> bool {
        matches!(
            self,
            CloseCode::Status
                | CloseCode::Tls
                | CloseCode::Abnormal
                | CloseCode::ReservedExtension(_)
        )
    }
}

#[derive(Error, Debug)]
#[error("Unknown close code: `{0}`")]
pub struct CloseCodeParseErr(pub(crate) u16);

impl TryFrom<[u8; 2]> for CloseCode {
    type Error = CloseCodeParseErr;

    fn try_from(value: [u8; 2]) -> Result<Self, Self::Error> {
        let value = u16::from_be_bytes(value);
        match value {
            n @ 0..=999 => Err(CloseCodeParseErr(n)),
            1000 => Ok(CloseCode::Normal),
            1001 => Ok(CloseCode::GoingAway),
            1002 => Ok(CloseCode::Protocol),
            1003 => Ok(CloseCode::Unsupported),
            1005 => Ok(CloseCode::Status),
            1006 => Ok(CloseCode::Abnormal),
            1007 => Ok(CloseCode::Invalid),
            1008 => Ok(CloseCode::Policy),
            1009 => Ok(CloseCode::Overflow),
            1010 => Ok(CloseCode::Extension),
            1011 => Ok(CloseCode::Unexpected),
            1012 => Ok(CloseCode::Restarting),
            1013 => Ok(CloseCode::TryAgain),
            1015 => Ok(CloseCode::Tls),
            n @ 1016..=1999 => Err(CloseCodeParseErr(n)),
            n @ 2000..=2999 => Ok(CloseCode::ReservedExtension(n)),
            n @ 3000..=3999 => Ok(CloseCode::Library(n)),
            n @ 4000..=4999 => Ok(CloseCode::Application(n)),
            n => Err(CloseCodeParseErr(n)),
        }
    }
}

impl From<CloseCode> for u16 {
    fn from(code: CloseCode) -> u16 {
        match code {
            CloseCode::Normal => 1000,
            CloseCode::GoingAway => 1001,
            CloseCode::Protocol => 1002,
            CloseCode::Unsupported => 1003,
            CloseCode::Status => 1005,
            CloseCode::Abnormal => 1006,
            CloseCode::Invalid => 1007,
            CloseCode::Policy => 1008,
            CloseCode::Overflow => 1009,
            CloseCode::Extension => 1010,
            CloseCode::Unexpected => 1011,
            CloseCode::Restarting => 1012,
            CloseCode::TryAgain => 1013,
            CloseCode::Tls => 1015,
            CloseCode::ReservedExtension(n) => n,
            CloseCode::Library(n) => n,
            CloseCode::Application(n) => n,
        }
    }
}
