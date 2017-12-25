pub enum SqlStatement {
    Statement(Statement),
    ExplainQueryPlan(Statement)
}

pub enum Statement {
    SelectStatement(SelectStatement),
}

pub struct SelectStatement {
    pub order_by: Vec<Ordering>,
    pub limit: Option<Limit>,
}

pub enum SelectMode {
    All,
    Distinct
}

pub enum SetExpression {
    Values(Vec<Vec<Box<Expression>>>),
    Query { mode: SelectMode, where_expr: Option<Box<Expression>> },
    Op { op: SetOperator, left: Box<SetExpression>, right: Box<SetExpression> },
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