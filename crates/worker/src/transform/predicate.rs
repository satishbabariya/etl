//! Subset-SQL predicate parser + evaluator.
//! Grammar:
//!   <predicate> ::= <col> IS [NOT] NULL
//!                 | <col> = <literal>
//!                 | <col> IN ( <literal-list> )

use anyhow::{Context, Result, bail};
use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Int(i64),
    String(String),
    Bool(bool),
    Null,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    IsNull(String),
    IsNotNull(String),
    Eq(String, Literal),
    In(String, Vec<Literal>),
}

pub fn parse(s: &str) -> Result<Predicate> {
    let toks = tokenize(s)?;
    let mut cursor = 0;
    let pred = parse_predicate(&toks, &mut cursor)?;
    if cursor != toks.len() {
        bail!("unexpected tokens after predicate at pos {cursor}");
    }
    Ok(pred)
}

#[derive(Debug, PartialEq)]
enum Tok {
    Ident(String),
    String(String),
    Int(i64),
    Eq,
    LParen,
    RParen,
    Comma,
    KwIs,
    KwNot,
    KwNull,
    KwTrue,
    KwFalse,
    KwIn,
}

fn tokenize(s: &str) -> Result<Vec<Tok>> {
    let chars: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '=' {
            out.push(Tok::Eq);
            i += 1;
        } else if c == '(' {
            out.push(Tok::LParen);
            i += 1;
        } else if c == ')' {
            out.push(Tok::RParen);
            i += 1;
        } else if c == ',' {
            out.push(Tok::Comma);
            i += 1;
        } else if c == '\'' {
            let mut j = i + 1;
            let mut buf = String::new();
            while j < chars.len() && chars[j] != '\'' {
                buf.push(chars[j]);
                j += 1;
            }
            if j == chars.len() {
                bail!("unterminated string literal at pos {i}");
            }
            out.push(Tok::String(buf));
            i = j + 1;
        } else if c.is_ascii_digit()
            || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit())
        {
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_ascii_digit() {
                j += 1;
            }
            let n: i64 = s[i..j]
                .parse()
                .with_context(|| format!("invalid int at {i}"))?;
            out.push(Tok::Int(n));
            i = j;
        } else if c.is_alphabetic() || c == '_' || c == '"' {
            let (ident, next) = read_ident(&chars, i)?;
            i = next;
            match ident.to_ascii_uppercase().as_str() {
                "IS" => out.push(Tok::KwIs),
                "NOT" => out.push(Tok::KwNot),
                "NULL" => out.push(Tok::KwNull),
                "TRUE" => out.push(Tok::KwTrue),
                "FALSE" => out.push(Tok::KwFalse),
                "IN" => out.push(Tok::KwIn),
                _ => out.push(Tok::Ident(ident)),
            }
        } else {
            bail!("unexpected character '{c}' at pos {i}");
        }
    }
    Ok(out)
}

fn read_ident(chars: &[char], start: usize) -> Result<(String, usize)> {
    if chars[start] == '"' {
        let mut j = start + 1;
        let mut buf = String::new();
        while j < chars.len() && chars[j] != '"' {
            buf.push(chars[j]);
            j += 1;
        }
        if j == chars.len() {
            bail!("unterminated quoted identifier");
        }
        Ok((buf, j + 1))
    } else {
        let mut j = start;
        while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
            j += 1;
        }
        Ok((chars[start..j].iter().collect(), j))
    }
}

fn parse_predicate(toks: &[Tok], c: &mut usize) -> Result<Predicate> {
    let col = match toks.get(*c) {
        Some(Tok::Ident(n)) => n.clone(),
        other => bail!("expected column name, got {other:?}"),
    };
    *c += 1;
    match toks.get(*c) {
        Some(Tok::KwIs) => {
            *c += 1;
            if matches!(toks.get(*c), Some(Tok::KwNot)) {
                *c += 1;
                expect(toks, c, &Tok::KwNull)?;
                Ok(Predicate::IsNotNull(col))
            } else {
                expect(toks, c, &Tok::KwNull)?;
                Ok(Predicate::IsNull(col))
            }
        }
        Some(Tok::Eq) => {
            *c += 1;
            let lit = parse_literal(toks, c)?;
            Ok(Predicate::Eq(col, lit))
        }
        Some(Tok::KwIn) => {
            *c += 1;
            expect(toks, c, &Tok::LParen)?;
            let mut items = Vec::new();
            loop {
                items.push(parse_literal(toks, c)?);
                match toks.get(*c) {
                    Some(Tok::Comma) => *c += 1,
                    Some(Tok::RParen) => {
                        *c += 1;
                        break;
                    }
                    other => bail!("expected , or ) in IN list, got {other:?}"),
                }
            }
            Ok(Predicate::In(col, items))
        }
        other => bail!("unsupported operator after column, got {other:?}"),
    }
}

fn parse_literal(toks: &[Tok], c: &mut usize) -> Result<Literal> {
    let lit = match toks.get(*c) {
        Some(Tok::Int(n)) => Literal::Int(*n),
        Some(Tok::String(s)) => Literal::String(s.clone()),
        Some(Tok::KwTrue) => Literal::Bool(true),
        Some(Tok::KwFalse) => Literal::Bool(false),
        Some(Tok::KwNull) => Literal::Null,
        other => bail!("expected literal, got {other:?}"),
    };
    *c += 1;
    Ok(lit)
}

fn expect(toks: &[Tok], c: &mut usize, want: &Tok) -> Result<()> {
    if toks.get(*c) == Some(want) {
        *c += 1;
        Ok(())
    } else {
        bail!("expected {:?}, got {:?}", want, toks.get(*c))
    }
}

pub fn evaluate(p: &Predicate, batch: &RecordBatch) -> Result<BooleanArray> {
    let col = |name: &str| -> Result<&dyn Array> {
        batch
            .column_by_name(name)
            .map(|a| a.as_ref())
            .ok_or_else(|| anyhow::anyhow!("column '{name}' not in batch"))
    };
    match p {
        Predicate::IsNull(name) => {
            let a = col(name)?;
            Ok(BooleanArray::from(
                (0..a.len()).map(|i| a.is_null(i)).collect::<Vec<_>>(),
            ))
        }
        Predicate::IsNotNull(name) => {
            let a = col(name)?;
            Ok(BooleanArray::from(
                (0..a.len()).map(|i| !a.is_null(i)).collect::<Vec<_>>(),
            ))
        }
        Predicate::Eq(name, lit) => eq_column(col(name)?, lit),
        Predicate::In(name, items) => {
            let a = col(name)?;
            let mut mask = vec![false; a.len()];
            for lit in items {
                let m = eq_column(a, lit)?;
                for (i, v) in m.iter().enumerate() {
                    mask[i] = mask[i] || v.unwrap_or(false);
                }
            }
            Ok(BooleanArray::from(mask))
        }
    }
}

fn eq_column(a: &dyn Array, lit: &Literal) -> Result<BooleanArray> {
    match lit {
        Literal::Int(n) => {
            let arr = a
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Eq Int expects Int64 column"))?;
            Ok(BooleanArray::from(
                (0..arr.len())
                    .map(|i| !arr.is_null(i) && arr.value(i) == *n)
                    .collect::<Vec<_>>(),
            ))
        }
        Literal::String(s) => {
            let arr = a
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Eq String expects Utf8 column"))?;
            Ok(BooleanArray::from(
                (0..arr.len())
                    .map(|i| !arr.is_null(i) && arr.value(i) == s.as_str())
                    .collect::<Vec<_>>(),
            ))
        }
        Literal::Null => Ok(BooleanArray::from(vec![false; a.len()])),
        Literal::Bool(_) => {
            anyhow::bail!("Eq Bool against non-Bool column (Phase I.5: not supported)")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("Alice"), None, Some("Carol")])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn parse_is_not_null() {
        assert_eq!(
            parse("email IS NOT NULL").unwrap(),
            Predicate::IsNotNull("email".into())
        );
    }
    #[test]
    fn parse_eq_int() {
        assert_eq!(
            parse("id = 42").unwrap(),
            Predicate::Eq("id".into(), Literal::Int(42))
        );
    }
    #[test]
    fn parse_eq_string() {
        assert_eq!(
            parse("status = 'active'").unwrap(),
            Predicate::Eq("status".into(), Literal::String("active".into()))
        );
    }
    #[test]
    fn parse_in_list() {
        assert_eq!(
            parse("status IN ('a', 'b', 'c')").unwrap(),
            Predicate::In(
                "status".into(),
                vec![
                    Literal::String("a".into()),
                    Literal::String("b".into()),
                    Literal::String("c".into()),
                ],
            )
        );
    }
    #[test]
    fn eval_is_not_null() {
        let b = batch();
        let m = evaluate(&Predicate::IsNotNull("name".into()), &b).unwrap();
        let v: Vec<bool> = m.iter().map(|o| o.unwrap()).collect();
        assert_eq!(v, vec![true, false, true]);
    }
    #[test]
    fn eval_eq_int() {
        let b = batch();
        let m = evaluate(&Predicate::Eq("id".into(), Literal::Int(3)), &b).unwrap();
        let v: Vec<bool> = m.iter().map(|o| o.unwrap()).collect();
        assert_eq!(v, vec![false, false, true]);
    }
    #[test]
    fn eval_in_list_strings() {
        let b = batch();
        let p = Predicate::In(
            "name".into(),
            vec![
                Literal::String("Alice".into()),
                Literal::String("Carol".into()),
            ],
        );
        let m = evaluate(&p, &b).unwrap();
        let v: Vec<bool> = m.iter().map(|o| o.unwrap()).collect();
        assert_eq!(v, vec![true, false, true]);
    }
}
