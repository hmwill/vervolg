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

use super::schema;

pub type Error = String;

pub enum SqlStatement {
    Statement(Statement),
    ExplainQueryPlan(Statement),
    Attach(AttachStatement),
    Describe(DescribeStatement),
}

impl SqlStatement {
    pub fn resolve_names(self, database: &schema::Database) -> Result<SqlStatement, Error> {
        match self {
            SqlStatement::Statement(statement) => Ok(SqlStatement::Statement(
                statement.resolve_names(&database)?,
            )),
            SqlStatement::ExplainQueryPlan(statement) => Ok(SqlStatement::ExplainQueryPlan(
                statement.resolve_names(&database)?,
            )),
            SqlStatement::Attach(statement) => Ok(SqlStatement::Attach(statement)),
            SqlStatement::Describe(statement) => Ok(SqlStatement::Describe(statement)),
        }
    }
}

pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    Update(UpdateStatement),
}

impl Statement {
    fn resolve_names(&self, database: &schema::Database) -> Result<Statement, Error> {
        match self {
            &Statement::Select(ref select) => Ok(
                Statement::Select(select.resolve_names(&database)?),
            ),
            &Statement::Insert(ref insert) => Ok(
                Statement::Insert(insert.resolve_names(&database)?),
            ),
            &Statement::Delete(ref delete) => Ok(
                Statement::Delete(delete.resolve_names(&database)?),
            ),
            &Statement::Update(ref update) => Ok(
                Statement::Update(update.resolve_names(&database)?),
            ),
        }
    }
}

pub struct InsertStatement {
    pub table_name: Vec<String>,
    pub columns: Option<Vec<String>>,
    pub source: Box<SetExpression>,
}

impl InsertStatement {
    fn resolve_names(&self, database: &schema::Database) -> Result<InsertStatement, Error> {
        // 1. validate the table name
        // 2. validate the column names
        // 3. validate the expression
        Err(String::from("Not implemented"))
    }
}

pub struct CommonTableExpression {
    pub identifier: String,
    pub column_names: Option<Vec<String>>,
    pub query: SelectStatement,
}

pub struct SelectStatement {
    pub common: Vec<CommonTableExpression>,
    pub expr: Box<SetExpression>,
    pub order_by: Vec<Ordering>,
    pub limit: Option<Limit>,
}

impl SelectStatement {
    fn resolve_names(&self, database: &schema::Database) -> Result<SelectStatement, Error> {
        Err(String::from("Not implemented"))
    }
}

pub struct DeleteStatement {
    pub table_name: Vec<String>,
    pub where_expr: Option<Box<Expression>>,
        // 1. validate the table name
        // 2. validate the expression
}

impl DeleteStatement {
    fn resolve_names(&self, database: &schema::Database) -> Result<DeleteStatement, Error> {
        Err(String::from("Not implemented"))
    }
}

pub struct UpdateStatement {
    pub table_name: Vec<String>,
    pub assignments: Vec<Assignment>,
    pub where_expr: Option<Box<Expression>>,
}

impl UpdateStatement {
    fn resolve_names(&self, database: &schema::Database) -> Result<UpdateStatement, Error> {
        // 1. validate the table name
        // 2. validate the assignments
        // 3. validate the where expression
        Err(String::from("Not implemented"))
    }
}

pub struct AttachStatement {
    pub schema: Option<String>,
    pub name: String,
    pub path: String,
}

impl AttachStatement {
    pub fn new(schema: Option<String>, name: String, path: String) -> AttachStatement {
        AttachStatement { schema, name, path }
    }
}

pub struct DescribeStatement {
    pub schema: Option<String>,
    pub name: String,
}

impl DescribeStatement {
    pub fn new(schema: Option<String>, name: String) -> DescribeStatement {
        DescribeStatement { schema, name }
    }
}

pub struct Assignment {
    pub columns: Vec<String>,
    pub expr: Box<Expression>,
}

pub enum SelectMode {
    All,
    Distinct,
}

pub enum SetExpression {
    Values(Vec<Vec<Box<Expression>>>),
    Query {
        mode: SelectMode,
        columns: ResultColumns,
        from: Vec<Box<TableExpression>>,
        where_expr: Option<Box<Expression>>,
        group_by: Option<GroupBy>,
    },
    Op {
        op: SetOperator,
        left: Box<SetExpression>,
        right: Box<SetExpression>,
    },
}

pub enum TableExpression {
    Named {
        name: Vec<String>,
        alias: Option<String>,
    },
    Select {
        select: SelectStatement,
        alias: Option<String>,
    },
    Join {
        left: Box<TableExpression>,
        right: Box<TableExpression>,
        op: JoinOperator,
        constraint: JoinConstraint,
    },
}

pub enum JoinConstraint {
    Expr(Box<Expression>),
    Columns(Vec<String>),
}

pub enum JoinOperator {
    Join(JoinType),
    Natural(JoinType),
    Cross,
}

pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

pub enum ResultColumns {
    All,
    List(Vec<Box<ResultColumn>>),
}

pub enum ResultColumn {
    AllFrom(String),
    Expr {
        expr: Box<Expression>,
        rename: Option<String>,
    },
}

pub struct GroupBy {
    pub groupings: Vec<Box<Expression>>,
    pub having: Option<Box<Expression>>,
}

pub enum SetOperator {
    Intersect,
    Except,
    Union,
    UnionAll,
}

pub enum UnaryOperator {
    Negate,
    Not,
    IsNull,
}

pub enum BinaryOperator {
    Multiply,
    Divide,
    Add,
    Subtract,
    Concat,
    And,
    Or,
}

pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessEqual,
    GreaterThan,
    GreaterEqual,
    Like,
    Is,
    IsNot,
}

pub enum Expression {
    Literal(Literal),
    QualifiedIdentifier(Vec<String>),
    MakeTuple(Vec<Box<Expression>>),
    Select(SelectStatement),

    Unary {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    Binary {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Comparison {
        op: ComparisonOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },

    In {
        expr: Box<Expression>,
        set: SetSpecification,
    },
    Between {
        expr: Box<Expression>,
        lower: Box<Expression>,
        upper: Box<Expression>,
    },

    Case {
        expr: Box<Expression>,
        when_part: Vec<WhenClause>,
        else_part: Option<Box<Expression>>,
    },
}

pub enum SetSpecification {
    Select(SelectStatement),
    List(Vec<Box<Expression>>),
    Name(Vec<String>),
}

pub struct WhenClause {
    pub guard: Box<Expression>,
    pub body: Box<Expression>,
}

pub enum Literal {
    StringLiteral(String),
    NumericLiteral(String),
    Null,
    CurrentTime,
    CurrentDate,
    CurrentTimestamp,
}

pub enum OrderingDirection {
    Ascending,
    Descending,
}

pub struct Ordering {
    pub expr: Box<Expression>,
    pub collation: Option<String>,
    pub direction: OrderingDirection,
}

pub struct Limit {
    pub number_rows: Box<Expression>,
    pub offset_value: Option<Box<Expression>>,
}

pub fn append<T>(list: Vec<T>, item: T) -> Vec<T> {
    let mut result = list;
    result.push(item);
    result
}
