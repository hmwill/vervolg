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

use ast;
use schema;
use session;
use Error;

lalrpop_mod!(pub sql);

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
        EmptyRowSet {
            meta_data: schema::RowSet::empty(),
        }
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

                result.push_field(column.name.as_str());
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

struct FullTableScanRowSet {
    table_name: String,
    meta_data: schema::RowSet,
}

impl FullTableScanRowSet {
    fn new(table_name: String, meta_data: schema::RowSet) -> Self {
        FullTableScanRowSet {
            table_name,
            meta_data,
        }
    }
}

impl RowSet for FullTableScanRowSet {
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

struct LiteralRowSet {
    meta_data: schema::RowSet,
}

impl LiteralRowSet {
    fn new(meta_data: schema::RowSet) -> Self {
        LiteralRowSet { meta_data }
    }
}

impl RowSet for LiteralRowSet {
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

struct FilterRowSet {
    meta_data: schema::RowSet,
}

impl FilterRowSet {
    fn new(meta_data: schema::RowSet) -> Self {
        FilterRowSet { meta_data }
    }
}

impl RowSet for FilterRowSet {
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

struct ProjectRowSet {
    meta_data: schema::RowSet,
}

impl ProjectRowSet {
    fn new(meta_data: schema::RowSet) -> Self {
        ProjectRowSet { meta_data }
    }
}

impl RowSet for ProjectRowSet {
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

struct AggregateRowSet {
    meta_data: schema::RowSet,
}

impl AggregateRowSet {
    fn new(meta_data: schema::RowSet) -> Self {
        AggregateRowSet { meta_data }
    }
}

impl RowSet for AggregateRowSet {
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

struct GroupByRowSet {
    meta_data: schema::RowSet,
}

impl GroupByRowSet {
    fn new(meta_data: schema::RowSet) -> Self {
        GroupByRowSet { meta_data }
    }
}

impl RowSet for GroupByRowSet {
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

struct SortRowSet {
    meta_data: schema::RowSet,
}

impl SortRowSet {
    fn new(meta_data: schema::RowSet) -> Self {
        SortRowSet { meta_data }
    }
}

impl RowSet for SortRowSet {
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

struct LimitRowSet {
    nested: Box<dyn RowSet>,
    counter: usize,
    offset: usize,
    limit: usize,
}

impl LimitRowSet {
    fn new(nested: Box<dyn RowSet>, limit: usize, offset: usize) -> Self {
        LimitRowSet {
            nested,
            counter: 0,
            limit,
            offset,
        }
    }
}

impl RowSet for LimitRowSet {
    fn reset(&mut self) -> Result<(), Error> {
        self.counter = 0;
        self.nested.reset()
    }

    fn next<'a>(&'a mut self) -> Option<RowResult<'a>> {
        // skip until the first row to return
        while self.counter < self.offset {
            let result = self.nested.next();

            if result.is_none() {
                return None;
            }

            self.counter += 1;
        }

        // have we reached the overall limit of rows to return?
        if self.counter >= self.offset + self.limit {
            return None;
        }

        let result = self.nested.next();

        if result.is_some() {
            self.counter += 1;
        }

        result
    }

    fn meta<'a>(&'a self) -> &'a schema::RowSet {
        &self.nested.meta()
    }
}

struct SortedJoinRowSet {
    meta_data: schema::RowSet,
}

impl SortedJoinRowSet {
    fn new(meta_data: schema::RowSet) -> Self {
        SortedJoinRowSet { meta_data }
    }
}

impl RowSet for SortedJoinRowSet {
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

/// An evaluation engine for SQL statements
pub struct Evaluator<'a> {
    session: &'a mut session::Session,
}

impl<'a> Evaluator<'a> {
    pub fn new(session: &'a mut session::Session) -> Evaluator<'a> {
        Evaluator { session }
    }

    pub fn eval(&mut self, command: &str) -> Result<Box<dyn RowSet>, Error> {
        let ast = self.parse(command)?;
        self.interpret(ast)
    }

    fn parse(&self, command: &str) -> Result<ast::SqlStatement, Error> {
        let parse_result = sql::SqlStatementParser::new().parse(command);

        match parse_result {
            Ok(statement) => Ok(statement),
            Err(err) => Err(Error::from(format!(
                "Input `{}`: parse error {:?}",
                command, err
            ))),
        }
    }

    fn interpret(&mut self, statement: ast::SqlStatement) -> Result<Box<dyn RowSet>, Error> {
        match statement {
            ast::SqlStatement::Statement(statement) => self.compile(statement),
            ast::SqlStatement::ExplainQueryPlan(statement) => {
                let _ = self.compile(statement)?;
                Err(Error::from("Explain not implemented yet!"))
            }
            ast::SqlStatement::Attach(info) => self.attach(info),
            ast::SqlStatement::Describe(info) => self.describe(info),
            ast::SqlStatement::AlterDomain(_) => unimplemented!("interpret for ALTER DOMAIN"),
            ast::SqlStatement::CreateDomain(_) => unimplemented!("interpret for CREATE DOMAIN"),
            ast::SqlStatement::DropDomain(_) => unimplemented!("interpret for DROP DOMAIN"),
            ast::SqlStatement::AlterTable(_) => unimplemented!("interpret for ALTER TABLE"),
            ast::SqlStatement::CreateTable(_) => unimplemented!("interpret for CREATE TABLE"),
            ast::SqlStatement::DropTable(_) => unimplemented!("interpret for DROP TABLE"),
            ast::SqlStatement::CreateSchema(_) => unimplemented!("interpret for CREATE SCHEMA"),
            ast::SqlStatement::DropSchema(_) => unimplemented!("interpret for DROP SCHEMA"),
            ast::SqlStatement::CreateView(_) => unimplemented!("interpret for CREATE VIEW"),
            ast::SqlStatement::DropView(_) => unimplemented!("interpret for DROP VIEW"),
        }
    }

    fn compile(&self, dml: ast::Statement) -> Result<Box<dyn RowSet>, Error> {
        match dml {
            ast::Statement::Select(select) => self.compile_select(select),
            _ => Err(Error::from(
                "Compile not implemented yet for these statement types!",
            )),
        }
    }

    fn compile_select(&self, select: ast::SelectStatement) -> Result<Box<dyn RowSet>, Error> {
        // compile the expression
        let rowset = self.compile_set_expression(&select.expr);

        // do we have a sorting clause?
        if !select.order_by.is_empty() {
            return Err(Error::from("Sorting not implemented yet"));
        }

        // do we have a limit clause?
        if select.limit.is_some() {
            let _limit = select.limit.unwrap();
            let offset = 0; // TODO implement evaluation of expression
            let row_count = 0; // TODO implement evaluation of expression
            Ok(Box::new(LimitRowSet::new(rowset?, offset, row_count)))
        } else {
            rowset
        }
    }

    fn compile_set_expression(&self, expr: &ast::SetExpression) -> Result<Box<dyn RowSet>, Error> {
        match expr {
            &ast::SetExpression::Values(ref values) => unimplemented!(),
            &ast::SetExpression::Op {
                ref op,
                ref left,
                ref right,
            } => unimplemented!(),
            &ast::SetExpression::Query {
                ref mode,
                ref columns,
                ref from,
                ref where_expr,
                ref group_by,
            } => {
                assert!(match mode {
                    &ast::SelectMode::All => true,
                    _ => false,
                });
                assert!(match columns {
                    &ast::ResultColumns::All => true,
                    _ => false,
                });
                assert!(where_expr.is_none());
                assert!(group_by.is_none());
                assert!(from.len() == 1);
                self.compile_table_expression(from[0].as_ref())
            }
        }
    }

    fn compile_table_expression(
        &self,
        expr: &ast::TableExpression,
    ) -> Result<Box<dyn RowSet>, Error> {
        match expr {
            &ast::TableExpression::Named {
                ref name,
                ref alias,
            } => unimplemented!(),
            &ast::TableExpression::Select {
                ref select,
                ref alias,
            } => unimplemented!(),
            &ast::TableExpression::Join {
                ref left,
                ref right,
                ref op,
                ref constraint,
            } => unimplemented!(),
        }
    }

    fn attach(&mut self, info: ast::AttachStatement) -> Result<Box<dyn RowSet>, Error> {
        self.session.database.attach_file(
            info.schema_name().unwrap_or(&self.session.default_schema),
            info.table_name(),
            &info.path,
        )?;
        Ok(Box::new(EmptyRowSet::new()))
    }

    fn describe(&mut self, info: ast::DescribeStatement) -> Result<Box<dyn RowSet>, Error> {
        let rowset = self.session.database.describe(
            info.schema_name().unwrap_or(&self.session.default_schema),
            info.table_name(),
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
