use std::cmp::Ordering;

use crate::codec::{SummaryValue, TreeKey, TreeSummary, TreeType, TreeValue};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Comparison {
    LT,
    LE,
    EQ,
    GE,
    GT,
}

pub(crate) use Comparison::*;

#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    clauses: Vec<QueryAnd>,
}

impl Query {
    pub(crate) fn new(position: usize, operator: Comparison, value: TreeValue) -> Query {
        let query = QueryCol {
            position,
            operator,
            value,
        };

        let query = QueryAnd {
            clauses: vec![query],
        };

        Query {
            clauses: vec![query],
        }
    }

    fn eval_scalar(&self, value: &TreeKey) -> bool {
        self.clauses.iter().any(|clause| clause.eval_scalar(value))
    }

    fn eval_summary(&self, summary: &TreeSummary) -> bool {
        self.clauses
            .iter()
            .any(|clause| clause.eval_summary(summary))
    }

    pub fn and(self, other: Query) -> Self {
        // (A || B) && (C || D) => (A && C) || (B && C) || (A && D) || (B && D)
        let mut or_clauses = vec![];

        for left in &self.clauses {
            for right in &other.clauses {
                let mut and_clauses = left.clauses.clone();
                and_clauses.extend(right.clauses.clone());

                or_clauses.push(QueryAnd {
                    clauses: and_clauses,
                });
            }
        }

        Self {
            clauses: or_clauses,
        }
    }

    pub fn or(mut self, other: Query) -> Self {
        self.clauses.extend(other.clauses);

        self
    }
}

#[derive(Clone, Debug, PartialEq)]
struct QueryAnd {
    clauses: Vec<QueryCol>,
}

impl QueryAnd {
    fn eval_scalar(&self, value: &TreeKey) -> bool {
        self.clauses.iter().all(|clause| clause.eval_scalar(value))
    }

    fn eval_summary(&self, summary: &TreeSummary) -> bool {
        self.clauses
            .iter()
            .all(|clause| clause.eval_summary(summary))
    }
}

#[derive(Clone, Debug, PartialEq)]
struct QueryCol {
    operator: Comparison,
    position: usize,
    value: TreeValue,
}

impl QueryCol {
    fn eval_scalar(&self, row: &TreeKey) -> bool {
        match row.get(self.position) {
            Some(value) => match self.operator {
                LE => *value <= self.value,
                LT => *value < self.value,
                EQ => *value == self.value,
                GT => *value > self.value,
                GE => *value >= self.value,
            },
            None => false,
        }
    }

    fn eval_summary(&self, summary: &TreeSummary) -> bool {
        match summary.get(self.position) {
            Some(summary) => match self.operator {
                LE => *summary <= self.value,
                LT => *summary < self.value,
                EQ => *summary == self.value,
                GT => *summary > self.value,
                GE => *summary >= self.value,
            },
            None => false,
        }
    }
}

impl PartialOrd for TreeValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            TreeValue::Timestamp(n) => n.partial_cmp(&i64::try_from(other.clone()).unwrap()),
            TreeValue::Integer(n) => n.partial_cmp(&i64::try_from(other.clone()).unwrap()),
            TreeValue::Float(n) => n.partial_cmp(&f64::try_from(other.clone()).unwrap()),
            TreeValue::String(s) => s.partial_cmp(&String::try_from(other.clone()).unwrap()),

            _ => unimplemented!(),
        }
    }
}

impl PartialEq<TreeValue> for SummaryValue {
    fn eq(&self, other: &TreeValue) -> bool {
        match self {
            SummaryValue::Timestamp(range) => {
                let other = i64::try_from(other.clone()).unwrap();
                !(other < range.lhs || other > range.rhs)
            }
            SummaryValue::Integer(range) => {
                let other = i64::try_from(other.clone()).unwrap();
                !(other < range.lhs || other > range.rhs)
            }
            SummaryValue::Float(range) => {
                let other = f64::try_from(other.clone()).unwrap();
                !(other < range.lhs || other > range.rhs)
            }
            SummaryValue::String(range) => {
                let other = String::try_from(other.clone()).unwrap();
                !(other < range.lhs || other > range.rhs)
            }
        }
    }
}

impl PartialOrd<TreeValue> for SummaryValue {
    fn partial_cmp(&self, _other: &TreeValue) -> Option<Ordering> {
        unimplemented!()
    }

    fn lt(&self, other: &TreeValue) -> bool {
        match self {
            SummaryValue::Timestamp(range) => {
                let other = i64::try_from(other.clone()).unwrap();
                range.lhs < other
            }
            SummaryValue::Integer(range) => {
                let other = i64::try_from(other.clone()).unwrap();
                range.lhs < other
            }
            SummaryValue::Float(range) => {
                let other = f64::try_from(other.clone()).unwrap();
                range.lhs < other
            }
            SummaryValue::String(range) => {
                let other = String::try_from(other.clone()).unwrap();
                range.lhs < other
            }
        }
    }

    fn le(&self, other: &TreeValue) -> bool {
        match self {
            SummaryValue::Timestamp(range) => {
                let other = i64::try_from(other.clone()).unwrap();
                range.lhs <= other
            }
            SummaryValue::Integer(range) => {
                let other = i64::try_from(other.clone()).unwrap();
                range.lhs <= other
            }
            SummaryValue::Float(range) => {
                let other = f64::try_from(other.clone()).unwrap();
                range.lhs <= other
            }
            SummaryValue::String(range) => {
                let other = String::try_from(other.clone()).unwrap();
                range.lhs <= other
            }
        }
    }

    fn gt(&self, other: &TreeValue) -> bool {
        match self {
            SummaryValue::Timestamp(range) => {
                let other = i64::try_from(other.clone()).unwrap();
                range.rhs > other
            }
            SummaryValue::Integer(range) => {
                let other = i64::try_from(other.clone()).unwrap();
                range.rhs > other
            }
            SummaryValue::Float(range) => {
                let other = f64::try_from(other.clone()).unwrap();
                range.rhs > other
            }
            SummaryValue::String(range) => {
                let other = String::try_from(other.clone()).unwrap();
                range.rhs > other
            }
        }
    }

    fn ge(&self, other: &TreeValue) -> bool {
        match self {
            SummaryValue::Timestamp(range) => {
                let other = i64::try_from(other.clone()).unwrap();
                range.rhs >= other
            }
            SummaryValue::Integer(range) => {
                let other = i64::try_from(other.clone()).unwrap();
                range.rhs >= other
            }
            SummaryValue::Float(range) => {
                let other = f64::try_from(other.clone()).unwrap();
                range.rhs >= other
            }
            SummaryValue::String(range) => {
                let other = String::try_from(other.clone()).unwrap();
                range.rhs >= other
            }
        }
    }
}

impl banyan::query::Query<TreeType> for Query {
    fn containing(
        &self,
        _offset: u64,
        index: &banyan::index::LeafIndex<TreeType>,
        res: &mut [bool],
    ) {
        for (i, key) in index.keys.as_ref().iter().enumerate() {
            res[i] = self.eval_scalar(key)
        }
    }

    fn intersecting(
        &self,
        _offset: u64,
        index: &banyan::index::BranchIndex<TreeType>,
        res: &mut [bool],
    ) {
        for (i, summary) in index.summaries.as_ref().iter().enumerate() {
            res[i] = self.eval_summary(summary)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::codec::{Row, SummaryRange, SummaryValue};

    use super::*;

    fn test_keys() -> Vec<TreeKey> {
        vec![
            Row(
                0b1010101010101010_0000000000000000_0000000000000000_0000000000000000.into(),
                vec![
                    TreeValue::Integer(16),
                    TreeValue::Float(3.141592),
                    TreeValue::Timestamp(1234),
                    TreeValue::String("Hi Mom!".into()),
                    TreeValue::Integer(42),
                    TreeValue::Float(2.71828),
                    TreeValue::String("Hello Dad!".into()),
                    TreeValue::Timestamp(5678),
                ],
            ),
            Row(
                0b0101010101010101_0000000000000000_0000000000000000_0000000000000000.into(),
                vec![
                    TreeValue::Integer(17),
                    TreeValue::Float(4.141592),
                    TreeValue::Timestamp(2345),
                    TreeValue::String("Hi Mo!".into()),
                    TreeValue::Integer(43),
                    TreeValue::Float(3.71828),
                    TreeValue::String("Hell Dad!".into()),
                    TreeValue::Timestamp(6789),
                ],
            ),
        ]
    }

    #[test]
    fn test_query_col_eval_scalar() {
        let row = &test_keys()[0];
        let q = |operator, position, value| {
            let query = QueryCol {
                operator,
                position,
                value,
            };

            query.eval_scalar(&row)
        };

        assert!(q(LT, 0, TreeValue::Integer(20)));
        assert!(!q(LT, 0, TreeValue::Integer(10)));
        assert!(!q(LT, 0, TreeValue::Integer(16)));

        assert!(q(LE, 0, TreeValue::Integer(20)));
        assert!(!q(LE, 0, TreeValue::Integer(10)));
        assert!(q(LE, 0, TreeValue::Integer(16)));

        assert!(!q(EQ, 0, TreeValue::Integer(20)));
        assert!(!q(EQ, 0, TreeValue::Integer(10)));
        assert!(q(EQ, 0, TreeValue::Integer(16)));

        assert!(!q(GE, 0, TreeValue::Integer(20)));
        assert!(q(GE, 0, TreeValue::Integer(10)));
        assert!(q(GE, 0, TreeValue::Integer(16)));

        assert!(!q(GT, 0, TreeValue::Integer(20)));
        assert!(q(GT, 0, TreeValue::Integer(10)));
        assert!(!q(GT, 0, TreeValue::Integer(16)));

        assert!(q(LT, 2, TreeValue::Float(4.2)));
        assert!(!q(LT, 2, TreeValue::Float(2.1)));
        assert!(!q(LT, 2, TreeValue::Float(3.141592)));

        assert!(q(LE, 2, TreeValue::Float(4.2)));
        assert!(!q(LE, 2, TreeValue::Float(2.1)));
        assert!(q(LE, 2, TreeValue::Float(3.141592)));

        assert!(!q(EQ, 2, TreeValue::Float(4.2)));
        assert!(!q(EQ, 2, TreeValue::Float(2.1)));
        assert!(q(EQ, 2, TreeValue::Float(3.141592)));

        assert!(!q(GE, 2, TreeValue::Float(4.2)));
        assert!(q(GE, 2, TreeValue::Float(2.1)));
        assert!(q(GE, 2, TreeValue::Float(3.141592)));

        assert!(!q(GT, 2, TreeValue::Float(4.2)));
        assert!(q(GT, 2, TreeValue::Float(2.1)));
        assert!(!q(GT, 2, TreeValue::Float(3.141592)));

        assert!(q(LT, 4, TreeValue::Timestamp(2000)));
        assert!(!q(LT, 4, TreeValue::Timestamp(1000)));
        assert!(!q(LT, 4, TreeValue::Timestamp(1234)));

        assert!(q(LE, 4, TreeValue::Timestamp(2000)));
        assert!(!q(LE, 4, TreeValue::Timestamp(1000)));
        assert!(q(LE, 4, TreeValue::Timestamp(1234)));

        assert!(!q(EQ, 4, TreeValue::Timestamp(2000)));
        assert!(!q(EQ, 4, TreeValue::Timestamp(1000)));
        assert!(q(EQ, 4, TreeValue::Timestamp(1234)));

        assert!(!q(GE, 4, TreeValue::Timestamp(2000)));
        assert!(q(GE, 4, TreeValue::Timestamp(1000)));
        assert!(q(GE, 4, TreeValue::Timestamp(1234)));

        assert!(!q(GT, 4, TreeValue::Timestamp(2000)));
        assert!(q(GT, 4, TreeValue::Timestamp(1000)));
        assert!(!q(GT, 4, TreeValue::Timestamp(1234)));

        assert!(q(LT, 6, TreeValue::String("Zebra".into())));
        assert!(!q(LT, 6, TreeValue::String("Aardvark".into())));
        assert!(!q(LT, 6, TreeValue::String("Hi Mom!".into())));

        assert!(q(LE, 6, TreeValue::String("Zebra".into())));
        assert!(!q(LE, 6, TreeValue::String("Aardvark".into())));
        assert!(q(LE, 6, TreeValue::String("Hi Mom!".into())));

        assert!(!q(EQ, 6, TreeValue::String("Zebra".into())));
        assert!(!q(EQ, 6, TreeValue::String("Aardvark".into())));
        assert!(q(EQ, 6, TreeValue::String("Hi Mom!".into())));

        assert!(!q(GE, 6, TreeValue::String("Zebra".into())));
        assert!(q(GE, 6, TreeValue::String("Aardvark".into())));
        assert!(q(GE, 6, TreeValue::String("Hi Mom!".into())));

        assert!(!q(GT, 6, TreeValue::String("Zebra".into())));
        assert!(q(GT, 6, TreeValue::String("Aardvark".into())));
        assert!(!q(GT, 6, TreeValue::String("Hi Mom!".into())));
    }

    fn test_summaries() -> Vec<TreeSummary> {
        vec![
            TreeSummary(
                0b1010101010101010_0000000000000000_0000000000000000_0000000000000000.into(),
                vec![
                    SummaryValue::Integer(SummaryRange { lhs: 16, rhs: 26 }),
                    SummaryValue::Float(SummaryRange {
                        lhs: 3.141592,
                        rhs: 3.541591,
                    }),
                    SummaryValue::Timestamp(SummaryRange {
                        lhs: 1234,
                        rhs: 2234,
                    }),
                    SummaryValue::String(SummaryRange {
                        lhs: "Hi Mom!".into(),
                        rhs: "Salve Madre".into(),
                    }),
                    SummaryValue::Integer(SummaryRange { lhs: 42, rhs: 52 }),
                    SummaryValue::Float(SummaryRange {
                        lhs: 2.71828,
                        rhs: 3.21828,
                    }),
                    SummaryValue::String(SummaryRange {
                        lhs: "Hello Dad!".into(),
                        rhs: "Salve Padre".into(),
                    }),
                    SummaryValue::Timestamp(SummaryRange {
                        lhs: 5678,
                        rhs: 6678,
                    }),
                ],
            ),
            TreeSummary(
                0b0101010101010101_0000000000000000_0000000000000000_0000000000000000.into(),
                vec![
                    SummaryValue::Integer(SummaryRange { lhs: 17, rhs: 27 }),
                    SummaryValue::Float(SummaryRange {
                        lhs: 4.141592,
                        rhs: 4.641592,
                    }),
                    SummaryValue::Timestamp(SummaryRange {
                        lhs: 2345,
                        rhs: 3345,
                    }),
                    SummaryValue::String(SummaryRange {
                        lhs: "Hi Mo!".into(),
                        rhs: "Hi Ze!".into(),
                    }),
                    SummaryValue::Integer(SummaryRange { lhs: 43, rhs: 53 }),
                    SummaryValue::Float(SummaryRange {
                        lhs: 3.71828,
                        rhs: 4.21828,
                    }),
                    SummaryValue::String(SummaryRange {
                        lhs: "Hell Dad!".into(),
                        rhs: "Hell Zap!".into(),
                    }),
                    SummaryValue::Timestamp(SummaryRange {
                        lhs: 6789,
                        rhs: 7789,
                    }),
                ],
            ),
        ]
    }

    #[test]
    fn test_query_col_eval_summary() {
        let summary = &test_summaries()[0];
        let q = |operator, position, value| {
            let query = QueryCol {
                operator,
                position,
                value,
            };
            query.eval_summary(summary)
        };

        assert!(!q(LT, 0, TreeValue::Integer(10)));
        assert!(!q(LT, 0, TreeValue::Integer(16)));
        assert!(q(LT, 0, TreeValue::Integer(20)));
        assert!(q(LT, 0, TreeValue::Integer(26)));
        assert!(q(LT, 0, TreeValue::Integer(30)));
    }

    #[test]
    fn test_query_algebra() {
        let a = QueryCol {
            position: 0,
            operator: EQ,
            value: TreeValue::Integer(16),
        };
        let b = QueryCol {
            position: 4,
            operator: GT,
            value: TreeValue::Timestamp(1000),
        };
        let c = QueryCol {
            position: 2,
            operator: EQ,
            value: TreeValue::Float(3.141592),
        };
        let d = QueryCol {
            position: 6,
            operator: EQ,
            value: TreeValue::String("Hi Mom!".into()),
        };

        fn q(q: QueryCol) -> Query {
            Query::new(q.position, q.operator, q.value)
        }

        let query = q(a.clone()).and(q(b.clone()));
        let expected = Query {
            clauses: vec![QueryAnd {
                clauses: vec![a.clone(), b.clone()],
            }],
        };
        assert_eq!(query, expected);

        let query = query.or(q(c.clone()));
        let expected = Query {
            clauses: vec![
                QueryAnd {
                    clauses: vec![a.clone(), b.clone()],
                },
                QueryAnd {
                    clauses: vec![c.clone()],
                },
            ],
        };
        assert_eq!(query, expected);

        let query = query.and(q(d.clone()));
        let expected = Query {
            clauses: vec![
                QueryAnd {
                    clauses: vec![a.clone(), b.clone(), d.clone()],
                },
                QueryAnd {
                    clauses: vec![c.clone(), d.clone()],
                },
            ],
        };
        assert_eq!(query, expected);

        let query1 = q(a.clone()).or(q(b.clone()));
        let query2 = q(c.clone()).or(q(d.clone()));
        let query = query1.and(query2);
        let expected = Query {
            clauses: vec![
                QueryAnd {
                    clauses: vec![a.clone(), c.clone()],
                },
                QueryAnd {
                    clauses: vec![a.clone(), d.clone()],
                },
                QueryAnd {
                    clauses: vec![b.clone(), c.clone()],
                },
                QueryAnd {
                    clauses: vec![b.clone(), d.clone()],
                },
            ],
        };
        assert_eq!(query, expected);
    }

    #[test]
    fn test_query_eval_scalar() {
        let keys = test_keys();
        let query = Query::new(0, EQ, TreeValue::Integer(16));
        assert!(query.eval_scalar(&keys[0]));
        assert!(!query.eval_scalar(&keys[1]));

        let query = query.or(Query::new(1, LT, TreeValue::Integer(100)));
        assert!(query.eval_scalar(&keys[0]));
        assert!(query.eval_scalar(&keys[1]));

        let query = query.and(Query::new(5, GT, TreeValue::Timestamp(1000)));
        assert!(!query.eval_scalar(&keys[0]));
        assert!(query.eval_scalar(&keys[1]));
    }

    #[test]
    fn test_query_eval_summary() {
        let summaries = test_summaries();
        let query = Query::new(0, EQ, TreeValue::Integer(16));
        assert!(query.eval_summary(&summaries[0]));
        assert!(!query.eval_summary(&summaries[1]));

        let query = query.or(Query::new(1, LT, TreeValue::Integer(100)));
        assert!(query.eval_summary(&summaries[0]));
        assert!(query.eval_summary(&summaries[1]));

        let query = query.and(Query::new(5, GT, TreeValue::Timestamp(1000)));
        assert!(!query.eval_summary(&summaries[0]));
        assert!(query.eval_summary(&summaries[1]));
    }
}
