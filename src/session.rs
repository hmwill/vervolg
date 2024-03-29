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

use users;

use super::schema;
use super::symbols;

/// Session state maintained for interactions with the database.
pub struct Session {
    /// The database object describing all known schemata
    pub database: schema::Database,

    /// The user name associated with this session
    pub user: String,

    /// The default schema associated with this session; for now, this is the same as the user name
    pub default_schema: symbols::Name,
}

impl Session {
    pub fn new() -> Session {
        let username = users::get_current_username().unwrap();
        let mut database = schema::Database::new();
        let schema_name = symbols::Name::from(&username[..]);

        database
            .create_schema(&schema_name)
            .expect("Database expected to be empty");

        Session {
            database,
            user: username,

            // we are using the OS user name as schema
            default_schema: schema_name,
        }
    }
}
