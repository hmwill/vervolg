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

pub enum SqlStatement {
    Statement(Statement),
    ExplainQueryPlan(Statement)
}

pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    Update(UpdateStatement),
}

pub struct InsertStatement {
    pub table_name: Vec<String>,
    pub columns: Option<Vec<String>>,
    pub source: Box<SetExpression>,
}

pub struct SelectStatement {
    pub expr: Box<SetExpression>,
    pub order_by: Vec<Ordering>,
    pub limit: Option<Limit>,
}

pub struct DeleteStatement {
    pub table_name: Vec<String>,
    pub where_expr: Option<Box<Expression>>,
}

pub struct UpdateStatement {
    pub table_name: Vec<String>,
    pub assignments: Vec<Assignment>,
    pub where_expr: Option<Box<Expression>>,
}

pub struct Assignment {
    pub columns: Vec<String>,
    pub expr: Box<Expression>
}

pub enum SelectMode {
    All,
    Distinct
}

pub enum SetExpression {
    Values(Vec<Vec<Box<Expression>>>),
    Query { mode: SelectMode, columns: ResultColumns, from: Vec<Box<TableExpression>>, where_expr: Option<Box<Expression>>, group_by: Option<GroupBy> },
    Op { op: SetOperator, left: Box<SetExpression>, right: Box<SetExpression> },
}

pub enum TableExpression {
    Named { name: Vec<String>, alias: Option<String> },
    Select { select: SelectStatement, alias: Option<String> },
    Join { left: Box<TableExpression>, right: Box<TableExpression>, op: JoinOperator, constraint: JoinConstraint },
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
    List(Vec<Box<ResultColumn>>)
}

pub enum ResultColumn {
    AllFrom(String),
    Expr { expr: Box<Expression>, rename: Option<String> },
}

pub struct GroupBy {
    pub groupings: Vec<Box<Expression>>,
    pub having: Option<Box<Expression>>
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

    Unary { op: UnaryOperator, expr: Box<Expression> },
    Binary { op: BinaryOperator, left: Box<Expression>, right: Box<Expression> },
    Comparison { op: ComparisonOperator, left: Box<Expression>, right: Box<Expression> },

    In { expr: Box<Expression>, set: SetSpecification },
    Between { expr: Box<Expression>, lower: Box<Expression>, upper: Box<Expression> },

    Case { expr: Box<Expression>, when_part: Vec<WhenClause>, else_part: Option<Box<Expression>> },
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
