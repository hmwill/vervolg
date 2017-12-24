pub enum SqlStatement {
    Statement(Statement),
    ExplainQueryPlan(Statement)
}

pub enum Statement {
    SelectStatement,
}