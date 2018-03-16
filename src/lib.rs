// MIT License
//
// Copyright (c) 2018 Hans-Martin Will
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

extern crate csv;
extern crate users;

pub mod ast;
pub mod sql;
pub mod schema;
pub mod types;
pub mod eval;
pub mod storage;
pub mod session;
pub mod algebra;

/// A simple error type for this library
pub struct Error {
    message: String,
    nested: Option<Box<std::error::Error>>,
}

impl Error {
    pub fn new<'a, E: 'static + std::error::Error>(message: &'a str, nested: Box<E>) -> Error {
        Error {
            message: String::from(message),
            nested: Some(nested),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.message)
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "[Error] {}", self.message)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        &self.message
    }
}

impl From<String> for Error {
    fn from(val: String) -> Error {
        Error {
            message: val,
            nested: None,
        }
    }
}

impl<'a> From<&'a str> for Error {
    fn from(val: &'a str) -> Error {
        Error {
            message: String::from(val),
            nested: None,
        }
    }
}
