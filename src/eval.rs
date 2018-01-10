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

use super::ast;
use super::schema;
use super::session;
use super::sql;
use super::Error;

use csv::StringRecord;

/// We are using the StringRecord type provided by the CSV library as row representation
pub type Row = StringRecord;
pub type RowResult<'a> = Result<&'a Row, Error>;

/// Query plan representation
pub struct QueryPlan {}

pub trait RowSet {
    fn reset(&mut self) -> Result<(), Error>;

    fn next<'a>(&'a mut self) -> Option<RowResult<'a>>;

    fn meta<'a>(&'a self) -> &'a schema::RowSet;
}

/// An empty row set that is returned when no result is needed
#[derive(Debug)]
struct EmptyRowSet {
    meta_data: schema::RowSet,
}

impl EmptyRowSet {
    fn new() -> Self {
        EmptyRowSet { meta_data: schema::RowSet::empty() }
    }
}

impl RowSet for EmptyRowSet {
    fn reset(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn next<'a>(&'a mut self) -> Option<RowResult<'a>> {
        None
    }

    fn meta<'a>(&'a self) -> &'a schema::RowSet {
        &self.meta_data
    }
}

/// A meta data row set that describes schema objects
struct MetaDataRowSet {
    index: usize,
    meta_data: schema::RowSet,
    columns: schema::RowSet,
    result_buffer: Row,
}

impl MetaDataRowSet {
    fn new(columns: schema::RowSet) -> Self {
        MetaDataRowSet {
            index: 0,
            columns,
            meta_data: schema::RowSet::meta_data(),
            result_buffer: Row::new(),
        }
    }
}

impl RowSet for MetaDataRowSet {
    fn reset(&mut self) -> Result<(), Error> {
        self.index = 0;
        Ok(())
    }

    fn next<'a>(&'a mut self) -> Option<RowResult<'a>> {
        let columns = &self.columns.columns;

        if self.index < columns.len() {
            let column = &columns[self.index];
            self.index += 1;

            {
                let result = &mut self.result_buffer;
                result.clear();

                result.push_field(&column.name);
                result.push_field(if column.not_null { "1" } else { "0" });
                result.push_field(if column.primary_key { "1" } else { "0" });
                result.push_field(&format!("{}", column.data_type));
            }

            Some(Ok(&self.result_buffer))
        } else {
            None
        }
    }

    fn meta<'a>(&'a self) -> &'a schema::RowSet {
        &self.meta_data
    }
}

/// An evaluation engine for SQL statements
pub struct Evaluator<'a> {
    session: &'a mut session::Session,
}

impl<'a> Evaluator<'a> {
    pub fn new(session: &'a mut session::Session) -> Evaluator<'a> {
        Evaluator { session }
    }

    pub fn eval(&mut self, command: &str) -> Result<Box<RowSet>, Error> {
        let ast = self.parse(command)?;
        self.interpret(ast)
    }

    fn parse(&self, command: &str) -> Result<ast::SqlStatement, Error> {
        let parse_result = sql::parse_SqlStatement(command);

        match parse_result {
            Ok(statement) => Ok(statement),
            Err(err) => Err(Error::from(
                format!("Input `{}`: parse error {:?}", command, err),
            )),
        }
    }

    fn interpret(&mut self, statement: ast::SqlStatement) -> Result<Box<RowSet>, Error> {
        match statement {
            ast::SqlStatement::Statement(statement) => {
                let plan = self.compile(statement)?;
                self.execute(plan)
            }
            ast::SqlStatement::ExplainQueryPlan(statement) => {
                let plan = self.compile(statement)?;
                Err(Error::from("Explain not implemented yet!"))
            }
            ast::SqlStatement::Attach(info) => self.attach(info),
            ast::SqlStatement::Describe(info) => self.describe(info),
        }
    }

    fn compile(&self, dml: ast::Statement) -> Result<QueryPlan, Error> {
        Err(Error::from("Compile not implemented yet!"))
    }

    fn execute(&self, plan: QueryPlan) -> Result<Box<RowSet>, Error> {
        Err(Error::from("Execute not implemented yet!"))
    }

    fn attach(&mut self, info: ast::AttachStatement) -> Result<Box<RowSet>, Error> {
        self.session.database.attach_file(
            info.schema.as_ref().unwrap_or(
                &self.session.default_schema,
            ),
            &info.name,
            &info.path,
        )?;
        Ok(Box::new(EmptyRowSet::new()))
    }

    fn describe(&mut self, info: ast::DescribeStatement) -> Result<Box<RowSet>, Error> {
        let rowset = self.session.database.describe(
            info.schema.as_ref().unwrap_or(
                &self.session.default_schema,
            ),
            &info.name,
        )?;
        Ok(Box::new(MetaDataRowSet::new(rowset)))
    }
}

/// An operator that used to construct query pipelines.
trait Operator {}

/*

Operator types:

- full table scan
- map
- filter
- sort
- accumulate
- group by
- sorted join
- limit

*/
