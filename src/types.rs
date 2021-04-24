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

use std::fmt;

/// The types supported by this engine
#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
pub enum DataType {
    /// Generic describes values without specific type constraints; will be parsed and converted on demand
    Generic,

    /// Character strings of varying length
    Varchar,

    /// Numeric values allowing for arithmetic operations; we'll add specializatiions later
    Numeric,

    /// Logical value (Boolean type)
    Logical,

    /// Date values
    Date,

    /// Time values
    Time,

    /// Timestamp values combining date and time information
    Timestamp,

    /// Geospatial data type
    Geometry,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &DataType::Generic => write!(f, "GENERIC"),
            &DataType::Logical => write!(f, "LOGICAL"),
            &DataType::Varchar => write!(f, "VARCHAR"),
            &DataType::Numeric => write!(f, "NUMERIC"),
            &DataType::Date => write!(f, "DATE"),
            &DataType::Time => write!(f, "TIME"),
            &DataType::Timestamp => write!(f, "TIMESTAMP"),
            &DataType::Geometry => write!(f, "GEOMETRY"),
        }
    }
}

impl DataType {
    /// Can values of this type be ordered?
    pub fn is_ordered(&self) -> bool {
        match self {
            &DataType::Varchar
            | &DataType::Numeric
            | &DataType::Date
            | &DataType::Time
            | &DataType::Timestamp => true,
            _ => false,
        }
    }
}
