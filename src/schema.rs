// MIT License
//
// Copyright (c) 2017 Hans-Martin Will
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

use std::collections::BTreeMap;
use std::path::PathBuf;

use csv;

use super::types;

/// the logical database, which is a collection of schemata
#[derive(Serialize, Deserialize, Debug)]
pub struct Database {
    pub schemata: BTreeMap<String, Schema>
}

impl Database {
    pub fn new() -> Database {
        Database {
            schemata: BTreeMap::new()
        }
    }
}

/// description of a schema within the database
#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    /// the schema name
    pub name: String,

    /// the collection of tables making up the database
    pub objects: BTreeMap<String, SchemaObject>,
}

/// currently, the only schema object types we support are tables and views
#[derive(Serialize, Deserialize, Debug)]
pub enum SchemaObject {
    /// a table object
    Table(Table),

    /// a view object
    View(View),
}

/// Description of a table within the database
#[derive(Serialize, Deserialize, Debug)]
pub struct Table {
    /// the name of the table
    pub name: String,

    /// description of the data rows that are stored in this table
    pub rows: RowSet,

    /// the names of the columns that are part of the primary key
    pub primary_key: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TableRepresentation {
    CsvFile { path: PathBuf },
}

/// Various options for the CSV library; ideally, this collection of parameters would reside within the
/// CSV library and could be passed directly to the csv::ReaderBuilder constructor.
pub struct CsvOptions {
    pub delimiter: u8,
    pub has_headers: bool,
    pub flexible: bool,
    pub terminator: csv::Terminator,
    pub quote: u8,
    pub escape: Option<u8>,
    pub double_quote: bool,
    pub quoting: bool,
    pub comment: Option<u8>,
    pub buffer_capacity: usize,
}

/// Description of a table within the database
#[derive(Serialize, Deserialize, Debug)]
pub struct View {
    /// the name of the view
    pub name: String,

    /// description of the data rows that are stored in this table
    pub rows: RowSet,

    /// description of the query used to generate the view (ultimately, this should be an AST)
    pub query: String,
}

/// Description of a collection of rows of the database
#[derive(Serialize, Deserialize, Debug)]
pub struct RowSet {
    /// the ordered list of columns in the database
    pub columns: Vec<Column>,
}

/// Description of a column with a data set
#[derive(Serialize, Deserialize, Debug)]
pub struct Column {
    /// the name of the column
    pub name: String,

    /// if true, the value is required
    pub not_null: bool,

    /// if true, the column is part of the primary key
    pub primary_key: bool,

    /// the type of the column
    pub data_type: types::DataType,
}