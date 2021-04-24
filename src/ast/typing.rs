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
use std::mem;

use super::*;

#[derive(Clone, Debug)]
pub enum ScalarType {
    Tuple(Vec<ScalarType>),

    Scalar { typ: types::DataType, is_null: bool },
}

impl ScalarType {
    fn is_compatible(&self, other: &ScalarType) -> Result<bool, Error> {
        match (self, other) {
            (
                &ScalarType::Scalar {
                    typ: typ1,
                    is_null: null1,
                },
                &ScalarType::Scalar {
                    typ: typ2,
                    is_null: null2,
                },
            ) => {
                if typ1 == typ2 {
                    Ok(null1 | null2)
                } else {
                    Err(Error::from("Incompatible types"))
                }
            }
            (&ScalarType::Tuple(ref tuple1), &ScalarType::Tuple(ref tuple2)) => {
                if tuple1.len() != tuple2.len() {
                    return Err(Error::from(
                        "Incompatible tuple types due to different lengths",
                    ));
                }

                tuple1
                    .iter()
                    .zip(tuple2.iter())
                    .fold(Ok(false), |result, (ref left, ref right)| {
                        let item_result = left.is_compatible(right);
                        if result.is_err() {
                            result
                        } else if item_result.is_err() {
                            item_result
                        } else {
                            Ok(result.unwrap() & item_result.unwrap())
                        }
                    })
            }
            _ => Err(Error::from("Incompatible types")),
        }
    }

    fn unify(&self, other: &ScalarType) -> Result<ScalarType, Error> {
        match (self, other) {
            (
                &ScalarType::Scalar {
                    typ: typ1,
                    is_null: null1,
                },
                &ScalarType::Scalar {
                    typ: typ2,
                    is_null: null2,
                },
            ) => {
                if typ1 == typ2 {
                    Ok(ScalarType::Scalar {
                        typ: typ1.clone(),
                        is_null: null1 | null2,
                    })
                } else {
                    Err(Error::from("Incompatible types"))
                }
            }
            (&ScalarType::Tuple(ref tuple1), &ScalarType::Tuple(ref tuple2)) => {
                if tuple1.len() != tuple2.len() {
                    return Err(Error::from(
                        "Incompatible tuple types due to different lengths",
                    ));
                }

                let components = fold_err(
                    Vec::new(),
                    tuple1.iter().zip(tuple2.iter()),
                    |mut vec, (ref left, ref right)| {
                        let item = left.unify(right)?;
                        vec.push(item);
                        Ok(vec)
                    },
                )?;
                Ok(ScalarType::Tuple(components))
            }
            _ => Err(Error::from("Incompatible types")),
        }
    }
}

enum ScalarContextSymbol {
    // a unique attribute that can be accessed directly
    Attribute(ScalarType),

    // a conflict of attribute from different relations
    AttributeConflict,

    // a relation with its constituent attributes
    Relation(collections::BTreeMap<symbols::Name, ScalarType>),
}

/// A context of bindings used to evaluate a type; for scalars, we are interested in bindings for
/// identifiers of attributes of rows within a relation.
pub struct ScalarContext<'a> {
    // reference to an underlying SetContext
    set_context: &'a SetContext,

    // need a way to capture qualified attributes and attributes without qualification
    symbols: collections::BTreeMap<symbols::Name, ScalarContextSymbol>,
}

impl<'a> ScalarContext<'a> {
    fn new(set_context: &'a SetContext) -> Self {
        ScalarContext {
            set_context,
            symbols: collections::BTreeMap::new(),
        }
    }

    fn add_relation(
        &mut self,
        name: symbols::Name,
        attributes: Vec<Attribute>,
    ) -> Result<(), Error> {
        // add the individual attributes
        for attribute in &attributes {
            match self.symbols.get(&attribute.name) {
                None => mem::drop(self.symbols.insert(
                    attribute.name.clone(),
                    ScalarContextSymbol::Attribute(ScalarType::Scalar {
                        typ: attribute.typ,
                        is_null: attribute.is_null,
                    }),
                )),
                Some(&ScalarContextSymbol::Attribute(_)) => mem::drop(self.symbols.insert(
                    attribute.name.clone(),
                    ScalarContextSymbol::AttributeConflict,
                )),
                Some(&ScalarContextSymbol::AttributeConflict) => (),
                Some(&ScalarContextSymbol::Relation(_)) => (),
            }
        }

        // add the relationship entry
        let map: collections::BTreeMap<symbols::Name, ScalarType> = attributes
            .into_iter()
            .map(|attribute| {
                (
                    attribute.name,
                    ScalarType::Scalar {
                        typ: attribute.typ,
                        is_null: attribute.is_null,
                    },
                )
            })
            .collect();

        match self.symbols.get(&name) {
            Some(&ScalarContextSymbol::Relation(_)) => {
                Err(Error::from(format!("Duplicate relation name '{}'", &name)))
            }
            _ => {
                mem::drop(
                    self.symbols
                        .insert(name, ScalarContextSymbol::Relation(map)),
                );
                Ok(())
            }
        }
    }

    fn infer_type(&self, qualified_name: &[symbols::Name]) -> Result<ScalarType, Error> {
        match qualified_name.len() {
            1 => match self.symbols.get(&qualified_name[0]) {
                None => Err(Error::from(format!(
                    "Undefined attribute name '{}'",
                    &qualified_name[0]
                ))),
                Some(&ScalarContextSymbol::Attribute(ref typ)) => Ok(typ.clone()),
                Some(&ScalarContextSymbol::AttributeConflict) => Err(Error::from(format!(
                    "Ambiguous attribute name '{}'",
                    &qualified_name[0]
                ))),
                Some(&ScalarContextSymbol::Relation(_)) => Err(Error::from(format!(
                    "Name '{}' refers to a relation",
                    &qualified_name[0]
                ))),
            },
            2 => match self.symbols.get(&qualified_name[0]) {
                Some(&ScalarContextSymbol::Relation(ref attributes)) => attributes
                    .get(&qualified_name[1])
                    .map(|t| t.clone())
                    .ok_or_else(|| {
                        Error::from(format!(
                            "Name '{}' does not refer to an attribute of relation '{}'",
                            &qualified_name[1], &qualified_name[0]
                        ))
                    }),
                _ => Err(Error::from(format!(
                    "Name '{}' does not refer to a relation",
                    &qualified_name[0]
                ))),
            },
            _ => Err(Error::from(
                "Qualfied name needs to have 1 or 2 path components",
            )),
        }
    }
}

impl Expression {
    fn infer_type(&self, context: &ScalarContext) -> Result<ScalarType, Error> {
        match self {
            &Expression::Literal(ref literal) => Ok(literal.infer_type()),

            /// a qualified name referring to an attribute of a bound relation
            &Expression::QualifiedIdentifier(ref qualident) => context.infer_type(qualident),

            /// tuple construction
            &Expression::MakeTuple(ref exprs) => {
                let result: Result<Vec<ScalarType>, Error> = exprs
                    .into_iter()
                    .map(|ref expr| expr.infer_type(context))
                    .collect();

                Ok(ScalarType::Tuple(result?))
            }

            /// unary operation
            &Expression::Unary { ref op, ref expr } => {
                let expr_type = expr.infer_type(context)?;

                match op {
                    &UnaryOperator::Negate => {
                        // should be a numeric type
                        match expr_type {
                            ScalarType::Scalar {
                                typ: types::DataType::Numeric,
                                is_null,
                            } => Ok(expr_type.clone()),
                            _ => Err(Error::from(
                                "Negate operation can only be applied to numeric type",
                            )),
                        }
                    }
                    &UnaryOperator::Not => {
                        // should be a logical type
                        match expr_type {
                            ScalarType::Scalar {
                                typ: types::DataType::Logical,
                                is_null,
                            } => Ok(expr_type.clone()),
                            _ => Err(Error::from(
                                "Not operation can only be applied to logical type",
                            )),
                        }
                    }
                    &UnaryOperator::IsNull => match expr_type {
                        ScalarType::Scalar { typ, is_null } => Ok(expr_type.clone()),
                        _ => Err(Error::from(
                            "Null test operation can only be applied to scalar type",
                        )),
                    },
                }
            }

            /// Binary operation
            &Expression::Binary {
                ref op,
                ref left,
                ref right,
            } => {
                let left_type = left.infer_type(context)?;
                let right_type = right.infer_type(context)?;

                match op {
                    &BinaryOperator::Multiply
                    | &BinaryOperator::Divide
                    | &BinaryOperator::Add
                    | &BinaryOperator::Subtract => {
                        // should be a numeric type
                        match (left_type, right_type) {
                            (
                                ScalarType::Scalar {
                                    typ: types::DataType::Numeric,
                                    is_null: null1,
                                },
                                ScalarType::Scalar {
                                    typ: types::DataType::Numeric,
                                    is_null: null2,
                                },
                            ) => Ok(ScalarType::Scalar {
                                typ: types::DataType::Numeric,
                                is_null: null1 | null2,
                            }),
                            _ => Err(Error::from(
                                "Arithmetic operation can only be applied to numeric type",
                            )),
                        }
                    }

                    &BinaryOperator::Concat => {
                        // should be a varchar type
                        match (left_type, right_type) {
                            (
                                ScalarType::Scalar {
                                    typ: types::DataType::Varchar,
                                    is_null: null1,
                                },
                                ScalarType::Scalar {
                                    typ: types::DataType::Varchar,
                                    is_null: null2,
                                },
                            ) => Ok(ScalarType::Scalar {
                                typ: types::DataType::Varchar,
                                is_null: null1 | null2,
                            }),
                            _ => Err(Error::from(
                                "Concat operation can only be applied to string type",
                            )),
                        }
                    }

                    &BinaryOperator::And | &BinaryOperator::Or => {
                        // should be a logical type
                        match (left_type, right_type) {
                            (
                                ScalarType::Scalar {
                                    typ: types::DataType::Logical,
                                    is_null: null1,
                                },
                                ScalarType::Scalar {
                                    typ: types::DataType::Logical,
                                    is_null: null2,
                                },
                            ) => Ok(ScalarType::Scalar {
                                typ: types::DataType::Logical,
                                is_null: null1 | null2,
                            }),
                            _ => Err(Error::from(
                                "Logical operation can only be applied to logical type",
                            )),
                        }
                    }
                }
            }

            /// Comparison operation
            &Expression::Comparison {
                ref op,
                ref left,
                ref right,
            } => {
                let left_type = left.infer_type(context)?;
                let right_type = right.infer_type(context)?;

                match op {
                    &ComparisonOperator::Equal | &ComparisonOperator::NotEqual => {
                        match left_type.is_compatible(&right_type) {
                            Ok(is_null) => Ok(ScalarType::Scalar {
                                typ: types::DataType::Logical,
                                is_null,
                            }),
                            Err(err) => Err(err),
                        }
                    }

                    &ComparisonOperator::LessThan
                    | &ComparisonOperator::LessEqual
                    | &ComparisonOperator::GreaterThan
                    | &ComparisonOperator::GreaterEqual => match (left_type, right_type) {
                        (
                            ScalarType::Scalar {
                                typ: typ1,
                                is_null: null1,
                            },
                            ScalarType::Scalar {
                                typ: typ2,
                                is_null: null2,
                            },
                        ) => {
                            if typ1 == typ2 && typ1.is_ordered() {
                                Ok(ScalarType::Scalar {
                                    typ: types::DataType::Logical,
                                    is_null: null1 | null2,
                                })
                            } else {
                                Err(Error::from("Operands of comparison operator need be of an ordered scalar type"))
                            }
                        }
                        _ => Err(Error::from(
                            "Operands of comparison operator need be of scalar type",
                        )),
                    },

                    &ComparisonOperator::Like => match (left_type, right_type) {
                        (
                            ScalarType::Scalar {
                                typ: types::DataType::Varchar,
                                is_null: null1,
                            },
                            ScalarType::Scalar {
                                typ: types::DataType::Varchar,
                                is_null: null2,
                            },
                        ) => Ok(ScalarType::Scalar {
                            typ: types::DataType::Logical,
                            is_null: null1 | null2,
                        }),
                        _ => Err(Error::from(
                            "Operands of Like operator need be of string type",
                        )),
                    },
                }
            }

            /// Range check
            &Expression::Between {
                ref expr,
                ref lower,
                ref upper,
            } => {
                let expr_type = expr.infer_type(context)?;
                let lower_type = lower.infer_type(context)?;
                let upper_type = upper.infer_type(context)?;

                match (expr_type, lower_type, upper_type) {
                    (
                        ScalarType::Scalar {
                            typ: typ1,
                            is_null: null1,
                        },
                        ScalarType::Scalar {
                            typ: typ2,
                            is_null: null2,
                        },
                        ScalarType::Scalar {
                            typ: typ3,
                            is_null: null3,
                        },
                    ) => {
                        if typ1 == typ2 && typ1 == typ3 && typ1.is_ordered() {
                            Ok(ScalarType::Scalar {
                                typ: types::DataType::Logical,
                                is_null: null1 | null2 | null3,
                            })
                        } else {
                            Err(Error::from("Operands of range check expression need be of an ordered scalar type"))
                        }
                    }
                    _ => Err(Error::from(
                        "Operands of range check expression need be of scalar type",
                    )),
                }
            }

            /// Case statement; guards are predicates
            &Expression::Case {
                expr: None,
                ref when_part,
                ref else_part,
            } => {
                // All when part clauses must have a guard that evaluates to a logical value
                let _ = fold_err((), when_part.iter(), |_, ref clause| {
                    let guard_type = clause.guard.infer_type(&context)?;

                    match guard_type {
                        ScalarType::Scalar {
                            typ: types::DataType::Logical,
                            is_null: _,
                        } => Ok(()),
                        _ => Err(Error::from("Case guard must evaluate to a logical type")),
                    }
                });

                // All when part clauses must have a body that evaluates to the same type
                assert!(when_part.len() >= 1);
                let mut iter = when_part.iter();
                let result_init_type = iter.next().unwrap().body.infer_type(&context)?;
                let result_type = fold_err(result_init_type, iter, |typ, ref clause| {
                    let body_type = clause.body.infer_type(&context)?;
                    let is_null1 = body_type.is_compatible(&typ)?;
                    match body_type {
                        ScalarType::Tuple(_) => Err(Error::from(
                            "THEN clause must evaluate to scalar expression",
                        )),
                        ScalarType::Scalar { typ, is_null } => Ok(ScalarType::Scalar {
                            typ,
                            is_null: is_null | is_null1,
                        }),
                    }
                })?;

                // If the else part is provided, it must be compatible with the type of all the clauses
                match (else_part, &result_type) {
                    (
                        &None,
                        &ScalarType::Scalar {
                            ref typ,
                            is_null: _,
                        },
                    ) => Ok(ScalarType::Scalar {
                        typ: typ.clone(),
                        is_null: true,
                    }),
                    (&Some(ref body), &ScalarType::Scalar { ref typ, is_null }) => {
                        let body_type = body.infer_type(&context)?;
                        let is_null1 = body_type.is_compatible(&result_type)?;
                        Ok(ScalarType::Scalar {
                            typ: *typ,
                            is_null: is_null1 | is_null,
                        })
                    }
                    _ => panic!("Should have covered all cases"),
                }
            }

            /// Case statement;switch on expr value
            &Expression::Case {
                expr: Some(ref expr),
                ref when_part,
                ref else_part,
            } => {
                // Type of expression must allow for scalar comparison
                let expr_type = expr.infer_type(&context)?;

                // All when part clauses must have a guard that evaluates to the same type as expr
                let _ = fold_err((), when_part.iter(), |_, ref clause| {
                    let guard_type = clause.guard.infer_type(&context)?;

                    if guard_type.is_compatible(&expr_type)? {
                        Ok(())
                    } else {
                        Err(Error::from(
                            "Guard expression is incompatible with CASE expression",
                        ))
                    }
                });

                // All when part clauses must have a body that evalujates to the same type
                assert!(when_part.len() >= 1);
                let mut iter = when_part.iter();
                let result_init_type = iter.next().unwrap().body.infer_type(&context)?;
                let result_type = fold_err(result_init_type, iter, |typ, ref clause| {
                    let body_type = clause.body.infer_type(&context)?;
                    let is_null1 = body_type.is_compatible(&typ)?;
                    match body_type {
                        ScalarType::Tuple(_) => Err(Error::from(
                            "THEN clause must evaluate to scalar expression",
                        )),
                        ScalarType::Scalar { typ, is_null } => Ok(ScalarType::Scalar {
                            typ,
                            is_null: is_null | is_null1,
                        }),
                    }
                })?;

                // If the else part is provided, it must be compatible with the type of all the clauses
                match (else_part, &result_type) {
                    (
                        &None,
                        &ScalarType::Scalar {
                            ref typ,
                            is_null: _,
                        },
                    ) => Ok(ScalarType::Scalar {
                        typ: typ.clone(),
                        is_null: true,
                    }),
                    (&Some(ref body), &ScalarType::Scalar { ref typ, is_null }) => {
                        let body_type = body.infer_type(&context)?;
                        let is_null1 = body_type.is_compatible(&result_type)?;
                        Ok(ScalarType::Scalar {
                            typ: *typ,
                            is_null: is_null1 | is_null,
                        })
                    }
                    _ => panic!("Should have covered all cases"),
                }
            }

            /// Set membership test; set should evaluate to a row set with a single column
            &Expression::In { ref expr, ref set } => {
                let expr_type = expr.infer_type(context)?;
                let set_type = set.infer_type(&context)?;

                if set_type.attributes.len() != 1 {
                    return Err(Error::from(
                        "Set for IN expression needs to evaluate to row set with a single column",
                    ));
                }

                if expr_type.is_compatible(&ScalarType::Scalar {
                    typ: set_type.attributes[0].typ,
                    is_null: set_type.attributes[0].is_null,
                })? {
                    Ok(ScalarType::Scalar {
                        typ: types::DataType::Logical,
                        is_null: false,
                    })
                } else {
                    Err(Error::from("Incompatible types for IN expression"))
                }
            }

            /// nested select statement
            ///
            /// should evaluate to a row set with a single column (and single row upon execution)
            &Expression::Select(ref select) => {
                let select_type = select.infer_type(&context.set_context)?;
                if select_type.attributes.len() == 1 {
                    Ok(ScalarType::Scalar {
                        typ: select_type.attributes[0].typ,
                        is_null: select_type.attributes[0].is_null,
                    })
                } else {
                    Err(Error::from(
                        "SELECT statement should evaluate to a single column",
                    ))
                }
            }
        }
    }
}

fn fold_err<S, I: iter::Iterator, F: Fn(S, I::Item) -> Result<S, Error>>(
    state: S,
    iter: I,
    func: F,
) -> Result<S, Error> {
    let mut result = state;

    for item in iter {
        match func(result, item) {
            Ok(val) => result = val,
            Err(err) => return Err(err),
        }
    }

    Ok(result)
}

impl Literal {
    fn infer_type(&self) -> ScalarType {
        match self {
            &Literal::StringLiteral(_) => ScalarType::Scalar {
                typ: types::DataType::Varchar,
                is_null: false,
            },

            /// Numeric literal
            &Literal::NumericLiteral(_) => ScalarType::Scalar {
                typ: types::DataType::Numeric,
                is_null: false,
            },

            /// the NULL value
            &Literal::Null => ScalarType::Scalar {
                typ: types::DataType::Generic,
                is_null: true,
            },

            /// the current time
            &Literal::CurrentTime => ScalarType::Scalar {
                typ: types::DataType::Time,
                is_null: false,
            },

            /// the current date
            &Literal::CurrentDate => ScalarType::Scalar {
                typ: types::DataType::Date,
                is_null: false,
            },

            /// the current timestamp
            &Literal::CurrentTimestamp => ScalarType::Scalar {
                typ: types::DataType::Timestamp,
                is_null: false,
            },

            /// DATE literal
            &Literal::DateLiteral(_) => ScalarType::Scalar {
                typ: types::DataType::Date,
                is_null: false,
            },

            /// TIME literal
            &Literal::TimeLiteral(_) => ScalarType::Scalar {
                typ: types::DataType::Time,
                is_null: false,
            },

            /// TIMESTAMP literal
            &Literal::TimestampLiteral(_) => ScalarType::Scalar {
                typ: types::DataType::Timestamp,
                is_null: false,
            },
        }
    }
}

#[derive(Clone, Debug)]
struct Attribute {
    name: symbols::Name,
    typ: types::DataType,
    is_null: bool,
}

impl Attribute {
    fn is_assignable_to(&self, other: &Attribute) -> bool {
        self.typ == other.typ && (other.is_null || !self.is_null)
    }
}

#[derive(Debug, Clone)]
pub struct RowType {
    attributes: Vec<Attribute>,
    primary_key: Vec<symbols::Name>,
    order_by: Vec<(symbols::Name, OrderingDirection)>,
}

impl<T> From<T> for RowType
where
    T: Borrow<schema::RowSet>,
{
    fn from(row_set: T) -> Self {
        let row_set_ref = row_set.borrow();

        let attributes: Vec<Attribute> = row_set_ref
            .columns
            .iter()
            .map(|column| Attribute {
                name: column.name.clone(),
                typ: column.data_type,
                is_null: !column.not_null,
            })
            .collect();

        let primary_key: Vec<symbols::Name> = row_set_ref
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|ref column| column.name.clone())
            .collect();

        RowType {
            attributes,
            primary_key,
            order_by: Vec::new(),
        }
    }
}

struct ObjectTypes(collections::BTreeMap<symbols::Name, RowType>);
struct Namespaces(collections::BTreeMap<symbols::Name, ObjectTypes>);

impl<T> From<T> for ObjectTypes
where
    T: Borrow<schema::Schema>,
{
    fn from(schema: T) -> Self {
        let schema_ref = schema.borrow();

        ObjectTypes(
            schema_ref
                .objects
                .iter()
                .map(|(name, ref schema_object)| {
                    (name.clone(), RowType::from(schema_object.row_set()))
                })
                .collect(),
        )
    }
}

impl<T> From<T> for Namespaces
where
    T: Borrow<schema::Database>,
{
    fn from(database: T) -> Self {
        let database_ref = database.borrow();

        Namespaces(
            database_ref
                .schemata
                .iter()
                .map(|(name, schema)| (name.clone(), ObjectTypes::from(schema)))
                .collect(),
        )
    }
}

/// A context of bindings used to evaluate a type; for set expressions, we are interested in bindings
/// of identifiers for relations.
pub struct SetContext {
    // need a reference to a database
    namespaces: Namespaces,

    // a default namespace name
    default_namespace: symbols::Name,

    // need a way to capture common table expression names
    table_expression: collections::BTreeMap<symbols::Name, RowType>,
}

impl SetContext {
    fn new(database: &schema::Database, default_schema: symbols::Name) -> Self {
        SetContext {
            namespaces: Namespaces::from(database),
            default_namespace: default_schema,
            table_expression: collections::BTreeMap::new(),
        }
    }

    fn define_relation(&mut self, name: symbols::Name, typ: RowType) -> Result<(), Error> {
        match self.table_expression.insert(name.clone(), typ) {
            None => Ok(()),
            Some(_) => Err(Error::from(format!(
                "Duplicate name '{}' for common table expression",
                name
            ))),
        }
    }

    fn resolve_table_name<'b>(
        &'b self,
        qualified_name: &[symbols::Name],
    ) -> Result<&'b RowType, Error> {
        match qualified_name.len() {
            1usize => {
                // Check for CTE
                match self.table_expression.get(&qualified_name[0]) {
                    Some(ref row_type) => return Ok(row_type),
                    None => (),
                }

                // look for the table name within the default schema
                match self.namespaces.0.get(&self.default_namespace) {
                    None => panic!("Default namespace not present in database"),
                    Some(ref namespace) => namespace.0.get(&qualified_name[0]).ok_or(Error::from(
                        format!("Table name {} not found in schema", qualified_name[0]),
                    )),
                }
            }
            2usize => {
                // Verify that the first component is not referencing a CTE
                match self.table_expression.get(&qualified_name[0]) {
                    Some(ref row_type) => {
                        return Err(Error::from(
                            "Schema name is hidden by common table expression",
                        ))
                    }
                    None => (),
                }

                // resolve a qualfied table name
                // look for the table name within the default schema
                match self.namespaces.0.get(&qualified_name[0]) {
                    None => Err(Error::from(format!(
                        "Namespace {} not present in database",
                        qualified_name[0]
                    ))),
                    Some(ref namespace) => namespace.0.get(&qualified_name[1]).ok_or(Error::from(
                        format!("Table name {} not found in schema", qualified_name[1]),
                    )),
                }
            }
            _ => Err(Error::from(
                "Table name references should have 1 or 2 components",
            )),
        }
    }

    // Convenience function to construct SetContext from Database and iterator over common table expressions
    fn initialize<'c, I>(
        database: &schema::Database,
        default_schema: symbols::Name,
        cte: I,
    ) -> Result<Self, Error>
    where
        I: iter::IntoIterator<Item = &'c CommonTableExpression>,
    {
        let mut result = Self::new(database, default_schema);

        for item in cte {
            let row_type = item.infer_type(&result)?;
            result.define_relation(item.identifier.clone(), row_type)?;
        }

        Ok(result)
    }
}

impl SetExpression {
    fn infer_type(&self, context: &SetContext) -> Result<RowType, Error> {
        match self {
            &SetExpression::Values(ref rows) => {
                let scalar_context = ScalarContext::new(context);
                assert!(rows.len() >= 1);
                let (mut component_types, _) =
                    fold_err((Vec::new(), 0), rows[0].iter(), |(mut vec, count), expr| {
                        let name = symbols::Name::from(format!("_col{}", count));
                        let expr_type = expr.infer_type(&scalar_context)?;

                        match expr_type {
                            ScalarType::Tuple(_) => {
                                Err(Error::from("Expecting a scalar expression"))
                            }
                            ScalarType::Scalar { typ, is_null } => {
                                vec.push(Attribute { name, typ, is_null });
                                Ok((vec, count + 1))
                            }
                        }
                    })?;

                // Validate other rows and unify into the result type
                let component_types =
                    fold_err(component_types, (&rows[1..]).iter(), |mut types, vals| {
                        fold_err(
                            Vec::new(),
                            types.iter().zip(vals.iter()),
                            |mut vec, (attr, expr)| {
                                let attribute_name = attr.name.clone();
                                let attribute_type = ScalarType::Scalar {
                                    typ: attr.typ,
                                    is_null: attr.is_null,
                                };
                                let expr_type = expr.infer_type(&scalar_context)?;
                                let component_type = attribute_type.unify(&expr_type)?;

                                match component_type {
                                    ScalarType::Scalar { typ, is_null } => {
                                        vec.push(Attribute {
                                            name: attribute_name,
                                            typ,
                                            is_null,
                                        });
                                        Ok(vec)
                                    }
                                    _ => panic!("Tuples are not valid components of rows"),
                                }
                            },
                        )
                    })?;

                Ok(RowType {
                    attributes: component_types,
                    primary_key: Vec::new(),
                    order_by: Vec::new(),
                })
            }
            &SetExpression::Query {
                ref mode,
                ref columns,
                ref from,
                ref where_expr,
                ref group_by,
            } => {
                // evaluate from => yielding a ScalarContext
                let (scalar_context, attributes) = fold_err(
                    (ScalarContext::new(context), Vec::new()),
                    from.iter(),
                    |(context, mut attributes), ref table_expr| {
                        let (context, mut new_attribiutes) = table_expr.infer_type(context)?;
                        attributes.append(&mut new_attribiutes);
                        Ok((context, attributes))
                    },
                )?;

                // evaluate where clause
                match where_expr {
                    &Some(ref expr) => match expr.infer_type(&scalar_context)? {
                        ScalarType::Scalar {
                            typ: types::DataType::Logical,
                            is_null: _,
                        } => (),
                        _ => {
                            return Err(Error::from(
                                "WHERE clause must evaluate to a BOOLEAN predicate",
                            ))
                        }
                    },
                    &None => (),
                };

                // evaluate group_by clause; just verify that those clauses refer to defined columns
                match group_by {
                    &None => (),
                    &Some(GroupBy {
                        ref groupings,
                        having: ref opt_having,
                    }) => {
                        for ref clause in groupings {
                            clause.infer_type(&scalar_context)?;
                        }

                        match opt_having {
                            &Some(ref having) => match having.infer_type(&scalar_context)? {
                                ScalarType::Scalar {
                                    typ: types::DataType::Logical,
                                    is_null,
                                } => (),
                                _ => {
                                    return Err(Error::from(
                                        "HAVING clause must evaluate to BOOLEAN value",
                                    ))
                                }
                            },
                            &None => (),
                        }
                    }
                };

                // evaluate columns; the default columns should be passed into this function
                columns.infer_type(&scalar_context, &attributes)
            }
            &SetExpression::Op {
                ref op,
                ref left,
                ref right,
            } => {
                let left_type = left.infer_type(context)?;
                let right_type = right.infer_type(context)?;

                // unify columns by position
                let attributes = fold_err(
                    Vec::new(),
                    left_type
                        .attributes
                        .iter()
                        .zip(right_type.attributes.iter()),
                    |mut vec, (ref left_attr, ref right_attr)| {
                        let name = left_attr.name.clone();
                        let left_attr_type = ScalarType::Scalar {
                            typ: left_attr.typ,
                            is_null: left_attr.is_null,
                        };
                        let right_attr_type = ScalarType::Scalar {
                            typ: right_attr.typ,
                            is_null: right_attr.is_null,
                        };
                        match left_attr_type.unify(&right_attr_type)? {
                            ScalarType::Scalar { typ, is_null } => {
                                vec.push(Attribute { name, typ, is_null });
                                Ok(vec)
                            }
                            _ => Err(Error::from("Embedded tuples are not allowed in row sets")),
                        }
                    },
                )?;

                Ok(RowType {
                    attributes,
                    primary_key: Vec::new(),
                    order_by: Vec::new(),
                })
            }
        }
    }
}

fn generate_column_name(
    name: &symbols::Name,
    generated_names: &mut collections::BTreeMap<symbols::Name, usize>,
) -> symbols::Name {
    if generated_names.contains_key(name) {
        let result = {
            let counter = generated_names.entry(name.clone()).or_insert(0usize);
            *counter += 1;
            symbols::Name::from(format!("{}{}", name, *counter))
        };

        generated_names.insert(result.clone(), 0usize);
        result
    } else {
        // first time we see the key
        let result = name.clone();
        generated_names.insert(result.clone(), 0usize);
        result
    }
}

impl ResultColumns {
    fn infer_type(
        &self,
        context: &ScalarContext,
        attributes: &[Attribute],
    ) -> Result<RowType, Error> {
        let mut result_attributes = Vec::new();

        // a map where we can track of names already generated
        let mut generated_columns = collections::BTreeMap::new();

        match self {
            &ResultColumns::All => for attribute in attributes {
                let result_name = generate_column_name(&attribute.name, &mut generated_columns);
                result_attributes.push(Attribute {
                    name: result_name,
                    typ: attribute.typ.clone(),
                    is_null: attribute.is_null,
                });
            },
            &ResultColumns::List(ref column_list) => for column in column_list {
                match &**column {
                    &ResultColumn::AllFrom(ref name) => match context.symbols.get(name) {
                        None => {
                            return Err(Error::from(format!("Undefined collection named {}", name)))
                        }
                        Some(&ScalarContextSymbol::Relation(ref fields)) => {
                            for (ref name, ref scalar) in fields {
                                match *scalar {
                                    &ScalarType::Scalar {
                                        ref typ,
                                        ref is_null,
                                    } => {
                                        let result_name =
                                            generate_column_name(name, &mut generated_columns);
                                        result_attributes.push(Attribute {
                                            name: result_name,
                                            typ: typ.clone(),
                                            is_null: *is_null,
                                        });
                                    }
                                    _ => panic!(
                                        "Attributes of a schema object should be scalar values"
                                    ),
                                }
                            }
                        }
                        Some(_) => {
                            return Err(Error::from(format!(
                                "Symbol {} needs to be a range variable",
                                name
                            )))
                        }
                    },
                    &ResultColumn::Expr {
                        expr: ref expr,
                        rename: ref alias,
                    } => match expr.infer_type(&context)? {
                        ScalarType::Tuple(_) => {
                            return Err(Error::from("Result column needs to be a scalar type"))
                        }
                        ScalarType::Scalar { typ, is_null } => {
                            let name = match alias {
                                &None => symbols::Name::from("col"),
                                &Some(ref name) => name.clone(),
                            };
                            result_attributes.push(Attribute {
                                name: generate_column_name(&name, &mut generated_columns),
                                typ,
                                is_null,
                            });
                        }
                    },
                }
            },
        }

        Ok(RowType {
            attributes: result_attributes,
            primary_key: Vec::new(),
            order_by: Vec::new(),
        })
    }
}

impl TableExpression {
    // The RowSet represents the columns that are visible in SELECT *; the ScalarContext provides
    // look-up information for any other expression that is to be evaluated.
    fn infer_type<'a>(
        &self,
        mut context: ScalarContext<'a>,
    ) -> Result<(ScalarContext<'a>, Vec<Attribute>), Error> {
        match self {
            &TableExpression::Named {
                ref name,
                ref alias,
            } => {
                // validate that the table name exists in the schema. If not => error
                let row_type = context.set_context.resolve_table_name(name)?;

                // If the alias is None, use the last component of the qualified table name
                // as short-hand alias
                let identifier = match alias {
                    &Some(ref id) => id.clone(),
                    &None => name.last().unwrap().clone(),
                };

                // register the table and its columns in the context
                context.add_relation(identifier, row_type.attributes.clone())?;

                // return the expanded context, as well as all the attributes as default result column list
                Ok((context, row_type.attributes.clone()))
            }
            &TableExpression::Select {
                ref select,
                ref alias,
            } => {
                let row_type = select.infer_type(&context.set_context)?;
                Ok((context, row_type.attributes))
            }
            &TableExpression::Join {
                ref left,
                ref right,
                ref op,
                ref constraint,
            } => {
                let (left_context, mut attributes) = left.infer_type(context)?;
                let (result_context, mut right_attributes) = right.infer_type(left_context)?;

                attributes.append(&mut right_attributes);
                Ok((result_context, attributes))
            }
        }
    }
}

impl SetSpecification {
    fn infer_type(&self, context: &ScalarContext) -> Result<RowType, Error> {
        match self {
            &SetSpecification::Select(ref select_statement) => {
                select_statement.infer_type(&context.set_context)
            }
            &SetSpecification::Name(ref qualified_name) => {
                let row_type = context
                    .set_context
                    .resolve_table_name(qualified_name.as_ref())?;
                Ok(row_type.clone())
            }
            &SetSpecification::List(ref values) => {
                let mut iter = values.iter();
                let initial_type = iter.next().unwrap().infer_type(&context);
                let value_type = iter.fold(initial_type, |typ, expr| {
                    let expr_type = expr.infer_type(&context)?;
                    typ?.unify(&expr_type)
                })?;

                match value_type {
                    ScalarType::Scalar { typ, is_null } => Ok(RowType {
                        attributes: vec![
                            Attribute {
                                name: symbols::Name::from("_col0"),
                                typ,
                                is_null,
                            },
                        ],
                        primary_key: Vec::new(),
                        order_by: Vec::new(),
                    }),
                    _ => Err(Error::from(
                        "Expressions for set specification must be of scalar type",
                    )),
                }
            }
        }
    }
}

impl SqlStatement {
    pub fn validate_type(&self, context: &SetContext) -> Result<(), Error> {
        match self {
            &SqlStatement::Statement(ref statement) => {
                let _ = statement.validate_type(context)?;
                Ok(())
            }
            &SqlStatement::ExplainQueryPlan(ref statement) => {
                let _ = statement.validate_type(context)?;
                Ok(())
            }
            &SqlStatement::Attach(ref attach) => attach.validate_type(context),
            &SqlStatement::Describe(ref describe) => describe.validate_type(context),
        }
    }
}

impl AttachStatement {
    fn validate_type(&self, context: &SetContext) -> Result<(), Error> {
        Ok(())
    }
}

impl DescribeStatement {
    fn validate_type(&self, context: &SetContext) -> Result<(), Error> {
        let _ = context.resolve_table_name(&self.qualified_name)?;
        Ok(())
    }
}

impl Statement {
    fn validate_type(&self, context: &SetContext) -> Result<(), Error> {
        match self {
            &Statement::Select(ref select) => {
                let _ = select.infer_type(context)?;
                Ok(())
            }
            &Statement::Insert(ref insert) => insert.validate_type(context),
            &Statement::Update(ref update) => update.validate_type(context),
            &Statement::Delete(ref delete) => delete.validate_type(context),
        }
    }
}

impl SelectStatement {
    fn infer_type(&self, context: &SetContext) -> Result<RowType, Error> {
        let mut scalar_context = ScalarContext::new(context);

        // common table expression; validate type of each expression and build up a ScalarConext
        for ref cte in &self.common {
            let cte_type = cte.infer_type(context)?;
            scalar_context.add_relation(cte.identifier.clone(), cte_type.attributes)?;
        }

        // set expression
        let result_type = self.expr.infer_type(context)?;

        // order by
        for ref iter in &self.order_by {
            iter.validate_type(&scalar_context)?;
        }

        // limit
        if let &Some(ref limit) = &self.limit {
            limit.validate_type(&scalar_context.set_context)?;
        }

        Ok(result_type)
    }
}

impl InsertStatement {
    fn validate_type(&self, context: &SetContext) -> Result<(), Error> {
        let row_type = context.resolve_table_name(&self.table_name)?;
        let mut row_type_map: collections::BTreeMap<symbols::Name, (types::DataType, bool)> =
            row_type
                .attributes
                .iter()
                .map(|attribute| (attribute.name.clone(), (attribute.typ, attribute.is_null)))
                .collect();

        let values_type = self.source.infer_type(&context)?;

        match &self.columns {
            &None => {
                // iterate over the value attriute types, and verify that the
                // associated type can be assigned to the corresponding column of the table row
                for ref attribute in &values_type.attributes {
                    match &row_type_map.get(&attribute.name) {
                        &None => {
                            return Err(Error::from(format!(
                                "Column '{}' not defined in table '{:?}'",
                                &attribute.name, &self.table_name
                            )))
                        }
                        &Some(&(ref typ, ref is_null)) => {
                            if *typ != attribute.typ || (!*is_null && attribute.is_null) {
                                return Err(Error::from(format!(
                                    "Cannot assign value '{}' to column '{}'",
                                    attribute.name, attribute.name
                                )));
                            }
                        }
                    }

                    row_type_map.remove(&attribute.name);
                }
            }
            &Some(ref columns) => {
                // vaidate that number of column align with values provided
                if columns.len() != values_type.attributes.len() {
                    return Err(Error::from(
                        "Length mismatch between column name list and value set provided",
                    ));
                }

                // iterate over the value attriute types, and verify that the
                // associated type can be assigned to the corresponding column of the table row
                for (ref attribute, name) in values_type.attributes.iter().zip(columns.iter()) {
                    match &row_type_map.get(name) {
                        &None => {
                            return Err(Error::from(format!(
                                "Column '{}' not defined in table '{:?}'",
                                &name, &self.table_name
                            )))
                        }
                        &Some(&(ref typ, ref is_null)) => {
                            if *typ != attribute.typ || (!*is_null && attribute.is_null) {
                                return Err(Error::from(format!(
                                    "Cannot assign value '{}' to column '{}'",
                                    attribute.name, name
                                )));
                            }
                        }
                    }

                    row_type_map.remove(name);
                }
            }
        }

        // also need to verify that we have provided values for all non-null columns of the table
        for (ref name, &(_, ref is_null)) in &row_type_map {
            if !is_null {
                return Err(Error::from(format!(
                    "Missing value for non-null column '{}'",
                    name
                )));
            }
        }

        Ok(())
    }
}

impl UpdateStatement {
    fn validate_type(&self, context: &SetContext) -> Result<(), Error> {
        // validate that the table name exists in the schema. If not => error
        let row_type = context.resolve_table_name(&self.table_name)?;
        let row_type_map: collections::BTreeMap<symbols::Name, (types::DataType, bool)> = row_type
            .attributes
            .iter()
            .map(|attribute| (attribute.name.clone(), (attribute.typ, attribute.is_null)))
            .collect();

        let identifier = self.table_name.last().unwrap().clone();
        let mut scalar_context = ScalarContext::new(&context);

        scalar_context.add_relation(identifier, row_type.attributes.clone())?;
        let mut assigned_columns = collections::BTreeSet::new();

        for assignment in &self.assignments {
            match assignment.expr.infer_type(&scalar_context)? {
                ScalarType::Scalar { typ, is_null } => for column in &assignment.columns {
                    if assigned_columns.contains(column) {
                        return Err(Error::from(format!(
                            "Duplicate assignment of column '{}'",
                            column
                        )));
                    }

                    match row_type_map.get(column) {
                        None => {
                            return Err(Error::from(format!(
                                "Assignment to undefined column '{}'",
                                column
                            )))
                        }
                        Some(&(ref column_type, ref colummn_is_null)) => {
                            if *column_type != typ || (is_null && !colummn_is_null) {
                                return Err(Error::from(format!(
                                    "Assignment incompatible value to column '{}'",
                                    column
                                )));
                            }
                        }
                    }

                    assigned_columns.insert(column);
                },
                _ => return Err(Error::from("Assigned value needs to be of scalar type")),
            }
        }

        match &self.where_expr {
            &Some(ref expr) => match expr.infer_type(&scalar_context)? {
                ScalarType::Scalar {
                    typ: types::DataType::Logical,
                    is_null: _,
                } => (),
                _ => {
                    return Err(Error::from(
                        "WHERE clause must evaluate to a BOOLEAN predicate",
                    ))
                }
            },
            &None => (),
        };

        Ok(())
    }
}

impl DeleteStatement {
    fn validate_type(&self, context: &SetContext) -> Result<(), Error> {
        // validate that the table name exists in the schema. If not => error
        let row_type = context.resolve_table_name(&self.table_name)?;
        let identifier = self.table_name.last().unwrap().clone();
        let mut scalar_context = ScalarContext::new(&context);
        scalar_context.add_relation(identifier, row_type.attributes.clone())?;

        match &self.where_expr {
            &Some(ref expr) => match expr.infer_type(&scalar_context)? {
                ScalarType::Scalar {
                    typ: types::DataType::Logical,
                    is_null: _,
                } => (),
                _ => {
                    return Err(Error::from(
                        "WHERE clause must evaluate to a BOOLEAN predicate",
                    ))
                }
            },
            &None => (),
        };

        Ok(())
    }
}

impl Ordering {
    fn validate_type(&self, context: &ScalarContext) -> Result<(), Error> {
        let typ = self.expr.infer_type(context)?;

        match typ {
            ScalarType::Scalar { typ: _, is_null: _ } => Ok(()),
            _ => Err(Error::from(
                "Expressions in ORDER BY clause need to be of scalar type",
            )),
        }
    }
}

impl Limit {
    fn validate_type(&self, context: &SetContext) -> Result<(), Error> {
        let scalar_context = ScalarContext::new(context);

        match self.number_rows.infer_type(&scalar_context)? {
            ScalarType::Scalar {
                typ: types::DataType::Numeric,
                is_null: false,
            } => (),
            _ => {
                return Err(Error::from(
                    "LIMIT expression must evaluate to non-null numeric value",
                ))
            }
        }

        if let &Some(ref offset_value) = &self.offset_value {
            match offset_value.infer_type(&scalar_context)? {
                ScalarType::Scalar {
                    typ: types::DataType::Numeric,
                    is_null: false,
                } => (),
                _ => {
                    return Err(Error::from(
                        "OFFSET expression must evaluate to non-null numeric value",
                    ))
                }
            }
        }

        Ok(())
    }
}

impl CommonTableExpression {
    fn infer_type(&self, context: &SetContext) -> Result<RowType, Error> {
        // query to determine row type
        let row_type = self.query.infer_type(context)?;

        // column_names to rearrange row type
        match &self.column_names {
            &None => Ok(row_type),
            &Some(ref column_names) => {
                let mut attributes = Vec::new();

                for column_name in column_names {
                    let index = row_type
                        .attributes
                        .iter()
                        .position(|column| column.name == *column_name)
                        .ok_or(Error::from(format!("Column {} not found", column_name)))?;
                    attributes.push(Attribute {
                        name: column_name.clone(),
                        typ: row_type.attributes[index].typ.clone(),
                        is_null: row_type.attributes[index].is_null,
                    })
                }

                Ok(RowType {
                    attributes,
                    primary_key: row_type.primary_key,
                    order_by: row_type.order_by,
                })
            }
        }
    }
}
