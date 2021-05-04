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

use super::schema;
use super::types;
use super::symbols;

use std::borrow::Borrow;

use std::collections;
use std::iter;

pub mod typing;

#[cfg(test)]
mod tests;

/// The error value; currently this is just a string
pub type Error = super::Error;

/// SQL statements that are supported by this implementation
pub enum SqlStatement {
    /// A regular (DML) statement
    Statement(Statement),

    /// Query plan analysis
    ExplainQueryPlan(Statement),

    /// Attach an external file as source for query processing
    Attach(AttachStatement),

    /// Describe a particular schema object
    Describe(DescribeStatement),
}

pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    Update(UpdateStatement),

    AlterDomain(AlterDomainStatement),
    CreateDomain(CreateDomainStatement),
    DropDomain(DropDomainStatement),

    CreateSchema(CreateSchemaStatement),
    DropSchema(DropSchemaStatement),

    AlterTable(AlterTableStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),

    CreateView(CreateViewStatement),
    DropView(DropViewStatement),
}

pub struct QualifiedName {
    pub schema: Option<symbols::Name>,
    pub name: symbols::Name,
}

pub struct CheckConstraint {
    pub name: Option<QualifiedName>,
    pub condition: Box<Expression>,
}

pub struct AlterDomainStatement { 
    pub name: QualifiedName,
    pub action: AlterDomainAction
}

pub enum AlterDomainAction {
    SetDefault(Literal),
    DropDefault,
    AddConstraint(CheckConstraint),
    DropConstraint(QualifiedName),
}

pub struct CreateDomainStatement { 
    pub name: QualifiedName,
    pub data_type: DataType,
    pub default: Option<Literal>,
    pub constraints: Vec<CheckConstraint>,
}

pub struct DropDomainStatement { 
    pub name: QualifiedName,
    pub drop_mode: DropMode
}

pub struct CreateSchemaStatement { 
    pub name: symbols::Name,
    pub authorization_user: Option<symbols::Name>,
    pub default_character_set: Option<QualifiedName>
}

#[derive(Clone, Copy, Debug)]
pub enum DropMode {
    Restrict,
    Cascade
}

pub struct DropSchemaStatement { 
    pub name: symbols::Name,
    pub drop_mode: DropMode,
}

pub struct AlterTableStatement { 
    pub name: QualifiedName,
    pub action: AlterTableAction,
}

pub enum AlterTableAction {
    AddColumn(ColumnDefinition),
    DropColumn { name: symbols::Name, drop_mode: DropMode },
    SetColumnDefault { name: symbols::Name, default: Literal },
    DropColumnDefault(symbols::Name),
    AddConstraint(TableConstraint),
    DropConstraint { name: QualifiedName, drop_mode: DropMode },
}

pub struct CreateTableStatement { 
    pub name: QualifiedName,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
}

pub struct ColumnDefinition {
    pub name: symbols::Name,
    pub column_type: ColumnType,
    pub default: Option<Literal>,
    pub constraints: Vec<ColumnConstraint>,
}

pub enum ColumnType {
    DataType(DataType),
    Domain(QualifiedName),
}

pub struct ColumnConstraint {
    pub name: Option<QualifiedName>,
    pub constraint: ColumnConstraintKind,
}

pub enum ColumnConstraintKind {
    NotNull,
    PrimaryKey,
    Unique,
    References(Reference),
    Check(Box<Expression>),
}

pub struct TableConstraint {
    pub name: Option<QualifiedName>,
    pub constraint: TableConstraintKind,
}

pub enum TableConstraintKind {
    Unique(Vec<symbols::Name>),
    PrimaryKey(Vec<symbols::Name>),
    ForeignKey { columns: Vec<symbols::Name>, reference: Reference },
    Check(Box<Expression>),
}

pub struct Reference {
    pub table: QualifiedName,
    pub columns: Option<Vec<symbols::Name>>,
    pub match_mode: Option<MatchMode>,
    pub on_update: Option<ReferentialAction>,
    pub on_delete: Option<ReferentialAction>
}

#[derive(Clone, Copy, Debug)]
pub enum ReferentialAction {
    NoAction,
    Cascade,
    SetDefault,
    SetNull
}

#[derive(Clone, Copy, Debug)]
pub enum MatchMode {
    Full,
    Partial
}

pub struct DropTableStatement { 
    pub name: QualifiedName,
    pub drop_mode: DropMode
}

pub struct CreateViewStatement { 
    pub name: QualifiedName,
    pub columns: Option<Vec<symbols::Name>>,
    pub expression: Box<TableExpression>,
    pub view_check_option: Option<ViewCheckOption>
}

#[derive(Clone, Copy, Debug)]
pub enum ViewCheckOption {
    Cascaded,
    Local
}

pub struct DropViewStatement { 
    pub name: QualifiedName,
    pub drop_mode: DropMode
}

/// Units assicated with interval types
#[derive(Clone, Copy, Debug)]
pub enum IntervalUnit {
    Second,
    Minute,
    Hour,
    Day,
    Month,
    Year
}

impl IntervalUnit {
    pub fn default_precision(self: &Self) -> PrecisionSpecification {
        match self {
            IntervalUnit::Second => PrecisionSpecification { digits: 2, fractions: 0 },
            IntervalUnit::Minute => PrecisionSpecification { digits: 2, fractions: 0 },
            IntervalUnit::Hour   => PrecisionSpecification { digits: 2, fractions: 0 },
            IntervalUnit::Day    => PrecisionSpecification { digits: 2, fractions: 0 },
            IntervalUnit::Month  => PrecisionSpecification { digits: 2, fractions: 0 },
            IntervalUnit::Year   => PrecisionSpecification { digits: 4, fractions: 0 },
        }
    }
}

/// Representation of digits . fractions
pub struct PrecisionSpecification {
    pub digits: usize,
    pub fractions: usize,
}

/// Specification of one end of and interval type specification
pub struct IntervalBound(pub IntervalUnit, pub PrecisionSpecification);

/// Representation of an interval type
pub struct IntervalType {
    pub from: IntervalBound,
    pub to: Option<IntervalBound>
}

/// Minimum required decimal digits for an INTEGER value, will round to 32 bit
pub const DECIMAL_INTEGER_DIGITS: usize = 9;

/// Minimum required decimal digits for a SMALLINT value, will round to 32 bit
pub const DECIMAL_SMALLINT_DIGITS: usize = 4;

pub const DEFAULT_NUMERIC_PRECISION: PrecisionSpecification = 
    PrecisionSpecification {
        digits: DECIMAL_INTEGER_DIGITS, 
        fractions: 0
    };

pub const DEFAULT_DECIMAL_PRECISION: PrecisionSpecification = 
    PrecisionSpecification {
        digits: DECIMAL_INTEGER_DIGITS, 
        fractions: 0
    };

pub const INTEGER_NUMERIC_PRECISION: PrecisionSpecification = 
    PrecisionSpecification {
        digits: DECIMAL_INTEGER_DIGITS, 
        fractions: 0
    };

pub const SMALLINT_DECIMAL_PRECISION: PrecisionSpecification = 
    PrecisionSpecification {
        digits: DECIMAL_SMALLINT_DIGITS, 
        fractions: 0
    };

/// Minimum required mantissa bits for a FLOAT value; corresponds to 32-bit IEEE float
pub const DEFAULT_FLOAT_MANTISSA: usize = 24;

/// Minimum required mantissa bits for a REAL value; corresponds to 32-bit IEEE float
pub const DEFAULT_REAL_MANTISSA: usize = DEFAULT_FLOAT_MANTISSA;

/// Minimum required mantissa bits for a DOUBLE PRECISION value; corresponds to 64-bit IEEE float
pub const DEFAULT_DOUBLE_PRECISION_MANTISSA: usize = 54;

/// Reresentation of a SQL data type
pub enum DataType {
    Boolean,
    Binary(usize),
    VarBinary(usize),
    Char(usize),
    VarChar(usize),
    Decimal(PrecisionSpecification),
    Numeric(PrecisionSpecification),
    Float(usize),
    Date,
    Time,
    Timestamp,
    Interval(IntervalType),
    Geometry(usize),
    Geography(usize),
}

/// Representation of an insert statement
pub struct InsertStatement {
    /// the name of the table into which we want to insert new values
    pub table_name: Vec<symbols::Name>,

    /// an optional list of columns that define a mapping between the provided values and the columns
    /// defined in the table
    pub columns: Option<Vec<symbols::Name>>,

    /// An expression that will yield the rows to insert
    pub source: Box<SetExpression>,
}

/// Representation of a common table expression, which provides a short-hand notation for
/// queries within the context of a single statement.
pub struct CommonTableExpression {
    /// the name under which we will refer to these query results in the remainder of the query
    /// that is using this common table expression
    pub identifier: symbols::Name,

    /// column names that can define a re-ordering of the values returned by the enclosed query
    pub column_names: Option<Vec<symbols::Name>>,

    /// a query statement that defines the values for this common table expression
    pub query: SelectStatement,
}

/// Representation of a select statement.
pub struct SelectStatement {
    /// 0 or more comon table expressions, that can be referenced by the main query expression
    pub common: Vec<CommonTableExpression>,

    /// the query expression
    pub expr: Box<SetExpression>,

    /// if non-empty, an sort-order that is applied to the rows returned as result
    pub order_by: Vec<Ordering>,

    /// an optional limit clause, which can restrict the rows returned to a window within the
    /// set of rows as generated by `expr` and `order_by`.
    pub limit: Option<Limit>,
}

/// Represenatation of a delete statement
pub struct DeleteStatement {
    /// the name of the table from which rows should be deleted
    pub table_name: Vec<symbols::Name>,

    /// a predicate defining the rows to delete
    pub where_expr: Option<Box<Expression>>,
}

/// Representation of an update statement
pub struct UpdateStatement {
    /// the qualified table name
    pub table_name: Vec<symbols::Name>,

    /// assignments providing new values for table columns
    pub assignments: Vec<Assignment>,

    /// a predicate restricting the set of rows to which the update should be applied
    pub where_expr: Option<Box<Expression>>,
}

/// Rerpresentation of an attach statement
pub struct AttachStatement {
    /// the table name within the previous (or default) schema
    pub qualified_name: Vec<symbols::Name>,

    /// the file system path of the external file to be attached as table
    pub path: String,
}

impl AttachStatement {
    pub fn new(
        schema: Option<symbols::Name>,
        name: symbols::Name,
        path: String,
    ) -> AttachStatement {
        let mut qualified_name = Vec::new();

        if schema.is_some() {
            qualified_name.push(schema.unwrap())
        }

        qualified_name.push(name);

        AttachStatement {
            qualified_name,
            path,
        }
    }

    pub fn schema_name(&self) -> Option<&symbols::Name> {
        match self.qualified_name.len() {
            2 => Some(&self.qualified_name[0]),
            1 => None,
            _ => panic!(),
        }
    }

    pub fn table_name(&self) -> &symbols::Name {
        &self.qualified_name.last().unwrap()
    }
}

/// Representation of a describe statememnt
pub struct DescribeStatement {
    /// the name of the object to describe
    pub qualified_name: Vec<symbols::Name>,
}

impl DescribeStatement {
    pub fn new(schema: Option<symbols::Name>, name: symbols::Name) -> DescribeStatement {
        let mut qualified_name = Vec::new();

        if schema.is_some() {
            qualified_name.push(schema.unwrap())
        }

        qualified_name.push(name);

        DescribeStatement { qualified_name }
    }

    pub fn schema_name(&self) -> Option<&symbols::Name> {
        match self.qualified_name.len() {
            2 => Some(&self.qualified_name[0]),
            1 => None,
            _ => panic!(),
        }
    }

    pub fn table_name(&self) -> &symbols::Name {
        &self.qualified_name.last().unwrap()
    }
}

/// Assignment used as part of an Update statement. One or more columns are updated with
/// the provided expression value.
pub struct Assignment {
    pub columns: Vec<symbols::Name>,
    pub expr: Box<Expression>,
}

pub enum SelectMode {
    All,
    Distinct,
}

/// Representation of a SetExpression, a collection of rows, each having one or more columns.
pub enum SetExpression {
    /// Literal row values
    Values(Vec<Vec<Box<Expression>>>),

    /// Query result as `SetExpression`
    Query {
        mode: SelectMode,
        columns: ResultColumns,
        from: Vec<Box<TableExpression>>,
        where_expr: Option<Box<Expression>>,
        group_by: Option<GroupBy>,
    },

    /// Binary operation on two `SetExpression` values
    Op {
        op: SetOperator,
        left: Box<SetExpression>,
        right: Box<SetExpression>,
    },
}

/// Representations of base queries
pub enum TableExpression {
    /// The row set of a given table; possibly providing an alias
    Named {
        /// the qualified table name
        name: Vec<symbols::Name>,

        /// an alias to refer to the row set within this expression
        alias: Option<symbols::Name>,
    },
    /// A nested select statement
    Select {
        /// a nested select statement
        select: SelectStatement,

        /// an alias to refer to the row set within this expression
        alias: Option<symbols::Name>,
    },
    /// The Join of two `TableExpression` values
    Join {
        /// the left table expression to join
        left: Box<TableExpression>,

        /// the right table expression to join
        right: Box<TableExpression>,

        /// the join operator
        op: JoinOperator,

        /// the join constraint, specifying what conditions need to apply for joining two rows
        constraint: JoinConstraint,
    },
}

/// Representation of a join constraint
pub enum JoinConstraint {
    /// an expression describing the contraint
    Expr(Box<Expression>),

    /// join constraints provided via column value constraints
    Columns(Vec<symbols::Name>),
}

/// Join operators
pub enum JoinOperator {
    /// Regular join
    Join(JoinType),

    /// Natural join
    Natural(JoinType),

    /// Cross join
    Cross,
}

/// Join types
pub enum JoinType {
    /// Inner join
    Inner,

    /// Left (outer) join
    Left,

    /// Right (outer) join
    Right,

    /// Full (outer) join
    Full,
}

/// Representation of result columns in a select statement
pub enum ResultColumns {
    /// All columns ('*')
    All,

    /// Result column specification
    List(Vec<Box<ResultColumn>>),
}

/// Representation of a single result column specification
pub enum ResultColumn {
    /// All columns from a given named schema object
    AllFrom(symbols::Name),

    /// An expression
    Expr {
        /// the expression to evaluate
        expr: Box<Expression>,

        /// an optional column name in the resulting row set
        rename: Option<symbols::Name>,
    },
}

/// Representation of grouping of result sets
pub struct GroupBy {
    /// One or more expressions that define the buckets for grouping
    pub groupings: Vec<Box<Expression>>,

    /// an optional constraint to limit the groups to collect rows for
    pub having: Option<Box<Expression>>,
}

/// Possible binary operators on row sets
pub enum SetOperator {
    /// Intersection operation
    Intersect,

    /// Set minus operation
    Except,

    /// Union of distinct values
    Union,

    /// Union including possible duplicates occuring on both sides
    UnionAll,
}

/// Possible unary operators for simple expressions
pub enum UnaryOperator {
    /// Numeric negation
    Negate,

    /// Logical inversion
    Not,

    /// Null check
    IsNull,
}

/// Binary operators for simple expressions
pub enum BinaryOperator {
    /// Numeric multiplication
    Multiply,

    /// Numeric division
    Divide,

    /// Numeric addition
    Add,

    /// Numeric subtraction
    Subtract,

    /// Concatenation of character sequences
    Concat,

    /// Logical and
    And,

    /// Logical or
    Or,
}

/// Comparison operators
pub enum ComparisonOperator {
    /// Equality
    Equal,

    /// Inquality
    NotEqual,

    /// Less than
    LessThan,

    /// Less than or equal to
    LessEqual,

    /// Greater than
    GreaterThan,

    /// Greater than or equal to
    GreaterEqual,

    /// Like operator (string matching)
    Like,
}

/// Scalar expressions
pub enum Expression {
    /// a literal value
    Literal(Literal),

    /// a qualified name referring to an attribute of a bound relation
    QualifiedIdentifier(Vec<symbols::Name>),

    /// tuple construction
    MakeTuple(Vec<Box<Expression>>),

    /// nested select statement
    Select(SelectStatement),

    /// unary operation
    Unary {
        op: UnaryOperator,
        expr: Box<Expression>,
    },

    /// Binary operation
    Binary {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },

    /// Comparison operation
    Comparison {
        op: ComparisonOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },

    /// Set membership test
    In {
        expr: Box<Expression>,
        set: SetSpecification,
    },

    /// Range check
    Between {
        expr: Box<Expression>,
        lower: Box<Expression>,
        upper: Box<Expression>,
    },

    /// Case statement
    Case {
        expr: Option<Box<Expression>>,
        when_part: Vec<WhenClause>,
        else_part: Option<Box<Expression>>,
    },
}

/// Specification of the containing set within a set membership expression
pub enum SetSpecification {
    /// Rows returned by a select statement
    Select(SelectStatement),

    /// List of expressions
    List(Vec<Box<Expression>>),

    /// a qualified name specifying a collection
    Name(Vec<symbols::Name>),
}

/// Representaion of a when clause used inside a case expression
pub struct WhenClause {
    /// guard statement determining when this claause applies
    pub guard: Box<Expression>,

    /// the guarded expression to evaluate when this clause applies
    pub body: Box<Expression>,
}

/// Literal values
pub enum Literal {
    /// String literal
    StringLiteral(String),

    /// Numeric literal
    NumericLiteral(String),

    /// the NULL value
    Null,

    /// the current time
    CurrentTime,

    /// the current date
    CurrentDate,

    /// the current timestamp
    CurrentTimestamp,

    /// DATE literal
    DateLiteral(String),

    /// TIME literal
    TimeLiteral(String),

    /// TIMESTAMP literal
    TimestampLiteral(String),
}

/// Sort ordering direction
#[derive(Debug, Clone)]
pub enum OrderingDirection {
    /// Sort in ascending order
    Ascending,

    /// Sort in descending order
    Descending,
}

/// Specification of a sort order
pub struct Ordering {
    /// an expression evaluating to the sort key
    pub expr: Box<Expression>,

    /// an optional collation to use for string comparisons
    pub collation: Option<symbols::Name>,

    /// Sort ordering direction
    pub direction: OrderingDirection,
}

/// Limits for a limit clause
pub struct Limit {
    /// number of rows to return
    pub number_rows: Box<Expression>,

    /// number of rows to skip
    pub offset_value: Option<Box<Expression>>,
}

/// Helper function to append an item to a vector
pub fn append<T>(list: Vec<T>, item: T) -> Vec<T> {
    let mut result = list;
    result.push(item);
    result
}
