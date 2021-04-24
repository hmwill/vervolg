// MIT License
//
// Copyright (c) 2018-2021 Hans-Martin Will
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

use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};
use std::fmt;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Name {
    string: String,
}

impl Name {
    pub fn new(string: String) -> Name {
        Name { string }
    }

    pub fn as_str(&self) -> &str {
        self.string.as_str()
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.string.fmt(f)
    }
}

impl<'a> From<&'a str> for Name {
    fn from(value: &'a str) -> Name {
        Name::new(String::from(value))
    }
}

impl From<String> for Name {
    fn from(value: String) -> Name {
        Name::new(value)
    }
}

impl PartialEq for Name {
    fn eq(&self, other: &Name) -> bool {
        let self_iter = self.string.chars().flat_map(|c| c.to_uppercase());
        let other_iter = other.string.chars().flat_map(|c| c.to_uppercase());
        self_iter.eq(other_iter)
    }
}

impl Eq for Name {}

impl PartialEq<str> for Name {
    fn eq(&self, other: &str) -> bool {
        let self_iter = self.string.chars().flat_map(|c| c.to_uppercase());
        let other_iter = other.chars().flat_map(|c| c.to_uppercase());
        self_iter.eq(other_iter)
    }
}

impl PartialOrd for Name {
    fn partial_cmp(&self, other: &Name) -> Option<Ordering> {
        let self_iter = self.string.chars().flat_map(|c| c.to_uppercase());
        let other_iter = other.string.chars().flat_map(|c| c.to_uppercase());
        self_iter.partial_cmp(other_iter)
    }
}

impl Ord for Name {
    fn cmp(&self, other: &Name) -> Ordering {
        let self_iter = self.string.chars().flat_map(|c| c.to_uppercase());
        let other_iter = other.string.chars().flat_map(|c| c.to_uppercase());
        self_iter.cmp(other_iter)
    }
}

impl PartialOrd<str> for Name {
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        let self_iter = self.string.chars().flat_map(|c| c.to_uppercase());
        let other_iter = other.chars().flat_map(|c| c.to_uppercase());
        self_iter.partial_cmp(other_iter)
    }
}
