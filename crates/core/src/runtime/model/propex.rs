use std::{
    borrow::Cow,
    fmt::Display,
    ops::{Deref, DerefMut},
    vec::Vec,
};

use thiserror::Error;

use crate::text::nom_parsers;

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::take_while1,
    character::complete::{char, digit1, multispace0},
    combinator::{all_consuming, map_res, opt},
    error::{ParseError, context},
    multi::{fold_many0, many1},
    sequence::{delimited, preceded},
};

#[derive(Error, Debug)]
pub enum PropexError {
    #[error("Invalid arguments")]
    BadArguments,

    #[error("Invalid Propex syntax, expr: `{0}`")]
    BadSyntax(String),

    #[error("Invalid number digit")]
    InvalidDigit,
}

#[derive(Debug, Clone)]
pub enum PropexSegment<'a> {
    Index(usize),
    Property(Cow<'a, str>), // Use a reference to a string slice
    Nested(Vec<PropexSegment<'a>>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum PropexPath<'a> {
    Single(PropexSegment<'a>),
    Multiple(Vec<[PropexSegment<'a>; 4]>),
}

impl<'a> PropexPath<'a> {
    pub fn as_slice(&self) -> &[PropexSegment<'a>] {
        match self {
            PropexPath::Single(segment) => std::slice::from_ref(segment),
            PropexPath::Multiple(segments) => segments.as_slice(),
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [PropexSegment<'a>] {
        match self {
            PropexPath::Single(segment) => std::slice::from_mut(segment),
            PropexPath::Multiple(segments) => segments.as_mut_slice(),
        }
    }
}

impl<'a> Deref for PropexPath<'a> {
    type Target = [PropexSegment<'a>];

    fn deref(&self) -> &Self::Target {
        match self {
            PropexPath::Single(segment) => std::slice::from_ref(segment),
            PropexPath::Multiple(segments) => segments.as_slice(),
        }
    }
}

impl<'a> DerefMut for PropexPath<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            PropexPath::Single(segment) => std::slice::from_mut(segment),
            PropexPath::Multiple(segments) => segments.as_mut_slice(),
        }
    }
}

impl<'a> PartialEq for PropexSegment<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PropexSegment::Index(i1), PropexSegment::Index(i2)) => i1 == i2,
            (PropexSegment::Property(p1), PropexSegment::Property(p2)) => p1 == p2,
            (PropexSegment::Nested(n1), PropexSegment::Nested(n2)) => n1 == n2,
            _ => false,
        }
    }
}

impl<'a> Eq for PropexSegment<'a> {}

impl<'a> Display for PropexSegment<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PropexSegment::Index(i) => write!(f, "[{i}]"),
            PropexSegment::Property(s) => write!(f, "[\"{s}\"]"),
            PropexSegment::Nested(n) => {
                write!(f, "[")?;
                for s in n.iter() {
                    write!(f, "{s}")?;
                }
                write!(f, "]")
            }
        }
    }
}

impl PropexSegment<'_> {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PropexSegment::Property(prop) => Some(prop),
            _ => None,
        }
    }

    pub fn as_index(&self) -> Option<usize> {
        match self {
            PropexSegment::Index(index) => Some(*index),
            _ => None,
        }
    }
}

pub fn token<'a, O, E: ParseError<&'a str>, G>(input: G) -> impl Parser<&'a str, Output = O, Error = E>
where
    G: Parser<&'a str, Output = O, Error = E>,
{
    delimited(multispace0, input, multispace0)
}

fn parse_usize(input: &str) -> IResult<&str, usize, nom::error::Error<&str>> {
    context("usize", map_res(digit1, |s: &str| s.parse::<usize>())).parse(input)
}

fn string_literal<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, &'a str, E> {
    let (input, quote) = alt((char('"'), char('\''))).parse(input)?;
    let (input, content) = take_while1(|c| c != quote).parse(input)?; // empty string is not allowed
    let (input, _) = char(quote).parse(input)?;
    Ok((input, content))
}

fn first_string_literal_property(i: &str) -> IResult<&str, PropexSegment, nom::error::Error<&str>> {
    token(string_literal).map(|x| PropexSegment::Property(Cow::Borrowed(x))).parse(i)
}

fn first_direct_property(i: &str) -> IResult<&str, PropexSegment, nom::error::Error<&str>> {
    nom_parsers::js_identifier.map(|x| PropexSegment::Property(Cow::Borrowed(x))).parse(i)
}

fn first_property(i: &str) -> IResult<&str, PropexSegment, nom::error::Error<&str>> {
    context(
        "first_property",
        alt((
            first_direct_property,         // `.abc`
            first_string_literal_property, // `'abc'` or `"abc"`
            quoted_index_property,         // `['abc']` or `["abc"]``
            //bracket_index,                 // `[123]`
            nested, // `[b.c]`
        )),
    )
    .parse(i)
}

/// `['prop']` or `["prop"]`
fn quoted_index_property(i: &str) -> IResult<&str, PropexSegment, nom::error::Error<&str>> {
    delimited(token(char('[')), string_literal, token(char(']')))
        .map(|x| PropexSegment::Property(Cow::Borrowed(x)))
        .parse(i)
}

/// `.property`
fn direct_identifier_property(i: &str) -> IResult<&str, PropexSegment, nom::error::Error<&str>> {
    context("direct_property", preceded(char('.'), nom_parsers::js_identifier))
        .map(|x: &str| PropexSegment::Property(Cow::Borrowed(x)))
        .parse(i)
}

/// `.123`
fn direct_numbers_index(i: &str) -> IResult<&str, PropexSegment, nom::error::Error<&str>> {
    context("direct_numbers_index", preceded(token(char('.')), token(parse_usize))).map(PropexSegment::Index).parse(i)
}

fn subproperty(i: &str) -> IResult<&str, PropexSegment, nom::error::Error<&str>> {
    context(
        "subproperty",
        alt((
            direct_identifier_property, // `a.b`
            direct_numbers_index,       // `a.123`
            quoted_index_property,      // `a["b"]`
            bracket_index,              // `a[123]`
            nested,                     // `a[b.c]`
        )),
    )
    .parse(i)
}

fn bracket_index(i: &str) -> IResult<&str, PropexSegment, nom::error::Error<&str>> {
    context("index", delimited(token(char('[')), token(parse_usize), token(char(']'))))
        .map(PropexSegment::Index)
        .parse(i)
}

fn nested(i: &str) -> IResult<&str, PropexSegment, nom::error::Error<&str>> {
    let (i, _) = token(char('[')).parse(i)?;
    let (i, first) = first_direct_property.parse(i)?;
    let (i, rest) = many1(subproperty).parse(i)?;
    let (i, _) = token(char(']')).parse(i)?;
    let mut result = Vec::with_capacity(rest.len() + 1);
    result.push(first);
    result.extend(rest);
    Ok((i, PropexSegment::Nested(result)))
}

fn expression(input: &str) -> IResult<&str, PropexPath, nom::error::Error<&str>> {
    let (input, first) = first_property.parse(input)?;

    let (input, rest) = context(
        "propex_expr",
        all_consuming(opt(fold_many0(subproperty, Vec::new, |mut acc, item| {
            acc.push(item);
            acc
        }))),
    )
    .parse(input)?;

    if let Some(rest) = rest {
        let mut result = Vec::with_capacity(rest.len() + 1);
        result.push(first);
        result.extend(rest);
        Ok((input, PropexPath::Multiple(result)))
    } else {
        Ok((input, PropexPath::Single(first)))
    }
}

pub fn parse(expr: &str) -> Result<PropexPath, PropexError> {
    if expr.is_empty() {
        return Err(PropexError::BadArguments);
    }
    match expression(expr) {
        Ok((_, segs)) => Ok(segs),
        Err(ve) => {
            log::debug!("{ve:?}");
            Err(PropexError::BadSyntax(expr.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn parse_primitives_should_be_ok() {
        let expr = "['test1']";
        let (_, parsed) = quoted_index_property(expr).unwrap();
        assert_eq!(PropexSegment::Property(Cow::Borrowed("test1")), parsed);

        let expr = r#"["test1"]"#;
        let (_, parsed) = quoted_index_property(expr).unwrap();
        assert_eq!(PropexSegment::Property(Cow::Borrowed("test1")), parsed);

        let expr = "_test_1";
        let (_, parsed) = first_direct_property(expr).unwrap();
        assert_eq!(PropexSegment::Property(Cow::Borrowed("_test_1")), parsed);

        let expr = ".foobar123";
        let (_, parsed) = direct_identifier_property(expr).unwrap();
        assert_eq!(PropexSegment::Property(Cow::Borrowed("foobar123")), parsed);

        let expr = "[ 'aaa']";
        let (_, parsed) = quoted_index_property(expr).unwrap();
        assert_eq!(PropexSegment::Property(Cow::Borrowed("aaa")), parsed);

        let expr = "[ 123 ]";
        let (_, parsed) = bracket_index(expr).unwrap();
        assert_eq!(PropexSegment::Index(123), parsed);
    }

    #[test]
    fn parse_propex_should_be_ok() {
        let expr1 = r#"test1.hello.world['aaa'][333]["bb"].name_of"#;
        let segs = parse(expr1).unwrap();

        assert_eq!(7, segs.len());
        assert_eq!(PropexSegment::Property(Cow::Borrowed("test1")), segs[0]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("hello")), segs[1]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("world")), segs[2]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("aaa")), segs[3]);
        assert_eq!(PropexSegment::Index(333), segs[4]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("bb")), segs[5]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("name_of")), segs[6]);
    }

    #[test]
    fn parse_propex_with_first_index_accessing_should_be_ok() {
        let expr1 = r#"['test1'].hello.world['aaa'].see[333]["bb"].name_of"#;
        let segs = parse(expr1).unwrap();

        assert_eq!(8, segs.len());
        assert_eq!(PropexSegment::Property(Cow::Borrowed("test1")), segs[0]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("hello")), segs[1]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("world")), segs[2]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("aaa")), segs[3]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("see")), segs[4]);
        assert_eq!(PropexSegment::Index(333), segs[5]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("bb")), segs[6]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("name_of")), segs[7]);
    }

    #[test]
    fn parse_propex_with_nested_propex() {
        let expr1 = r#"['test1'].msg.payload[msg["topic"][0]].str[123]"#;
        let segs = parse(expr1).unwrap();

        assert_eq!(6, segs.len());
        assert_eq!(PropexSegment::Property(Cow::Borrowed("test1")), segs[0]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("msg")), segs[1]);
        assert_eq!(PropexSegment::Property(Cow::Borrowed("payload")), segs[2]);
        assert_eq!(
            PropexSegment::Nested(vec![
                PropexSegment::Property(Cow::Borrowed("msg")),
                PropexSegment::Property(Cow::Borrowed("topic")),
                PropexSegment::Index(0)
            ]),
            segs[3]
        );
        assert_eq!(PropexSegment::Property(Cow::Borrowed("str")), segs[4]);
        assert_eq!(PropexSegment::Index(123), segs[5]);
    }

    /// from `node-red/test/unit/@node-red/util/lib/util_spec.js`
    #[test]
    fn should_pass_red_node_unit_tests() {
        use PropexSegment::*;
        assert_eq!(
            parse("a.b.c").unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Property(Cow::Borrowed("b")),
                Property(Cow::Borrowed("c"))
            ]),
            "pass a.b.c"
        );

        assert_eq!(
            parse(r#"a["b"]["c"]"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Property(Cow::Borrowed("b")),
                Property(Cow::Borrowed("c"))
            ]),
            r#"pass a["b"]["c"]"#
        );

        assert_eq!(
            parse(r#"a["b"].c"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Property(Cow::Borrowed("b")),
                Property(Cow::Borrowed("c"))
            ]),
            r#"pass a["b"].c"#
        );

        assert_eq!(
            parse(r#"a['b'].c"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Property(Cow::Borrowed("b")),
                Property(Cow::Borrowed("c"))
            ]),
            r#"pass a['b'].c"#
        );

        assert_eq!(
            parse(r#"a[0].c"#).unwrap(),
            PropexPath::Multiple(vec![Property(Cow::Borrowed("a")), Index(0), Property(Cow::Borrowed("c"))]),
            r#"pass a[0].c"#
        );

        assert_eq!(
            parse(r#"a.0.c"#).unwrap(),
            PropexPath::Multiple(vec![Property(Cow::Borrowed("a")), Index(0), Property(Cow::Borrowed("c"))]),
            r#"pass a.0.c"#
        );

        assert_eq!(
            parse(r#"a['a.b[0]'].c"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Property(Cow::Borrowed("a.b[0]")),
                Property(Cow::Borrowed("c"))
            ]),
            r#"pass a['a.b[0]'].c"#
        );

        assert_eq!(
            parse(r#"a[0][0][0]"#).unwrap(),
            PropexPath::Multiple(vec![Property(Cow::Borrowed("a")), Index(0), Index(0), Index(0)]),
            r#"pass a[0][0][0]"#
        );

        assert_eq!(
            parse(r#"'1.2.3.4'"#).unwrap(),
            PropexPath::Multiple(vec![Property(Cow::Borrowed("1.2.3.4")),]),
            r#"pass '1.2.3.4'"#
        );

        assert_eq!(
            parse(r#"'a.b'[1]"#).unwrap(),
            PropexPath::Multiple(vec![Property(Cow::Borrowed("a.b")), Index(1)]),
            r#"pass 'a.b'[1]"#
        );

        assert_eq!(
            parse(r#"'a.b'.c"#).unwrap(),
            PropexPath::Multiple(vec![Property(Cow::Borrowed("a.b")), Property(Cow::Borrowed("c"))]),
            r#"pass 'a.b'.c"#
        );

        assert_eq!(
            parse(r#"a[msg.b]"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Nested(vec![Property(Cow::Borrowed("msg")), Property(Cow::Borrowed("b"))])
            ]),
            r#"pass a[msg.b]"#
        );

        assert_eq!(
            parse(r#"a[msg[msg.b]]"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Nested(vec![
                    Property(Cow::Borrowed("msg")),
                    Nested(vec![Property(Cow::Borrowed("msg")), Property(Cow::Borrowed("b"))])
                ])
            ]),
            r#"pass a[msg[msg.b]]"#
        );

        assert_eq!(
            parse(r#"a[msg['b]"[']]"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Nested(vec![Property(Cow::Borrowed("msg")), Property(Cow::Borrowed(r#"b]"["#))])
            ]),
            r#"pass a[msg['b]"[']]"#
        );

        assert_eq!(
            parse(r#"a[msg['b][']]"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Nested(vec![Property(Cow::Borrowed("msg")), Property(Cow::Borrowed(r#"b]["#))])
            ]),
            r#"pass a[msg['b][']]"#
        );

        assert_eq!(
            parse(r#"b[msg.a][2]"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("b")),
                Nested(vec![Property(Cow::Borrowed("msg")), Property(Cow::Borrowed("a"))]),
                Index(2)
            ]),
            r#"pass b[msg.a][2]"#
        );

        assert_eq!(
            parse(r#"a.$b.c"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Property(Cow::Borrowed("$b")),
                Property(Cow::Borrowed("c"))
            ]),
            r#"pass a.$b.c"#
        );

        assert_eq!(
            parse(r#"a["$b"].c"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Property(Cow::Borrowed("$b")),
                Property(Cow::Borrowed("c"))
            ]),
            r#"pass a["$b"].c"#
        );

        assert_eq!(
            parse(r#"a._b.c"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Property(Cow::Borrowed("_b")),
                Property(Cow::Borrowed("c"))
            ]),
            r#"pass a._b.c"#
        );

        assert_eq!(
            parse(r#"a["_b"].c"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Property(Cow::Borrowed("_b")),
                Property(Cow::Borrowed("c"))
            ]),
            r#"pass a["_b"].c"#
        );

        assert_eq!(
            parse(r#"a['a.b[0]'].c"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Property(Cow::Borrowed("a.b[0]")),
                Property(Cow::Borrowed("c"))
            ]),
            r#"pass a['a.b[0]'].c"#
        );

        assert_eq!(
            parse(r#"a[msg.c][0]["fred"]"#).unwrap(),
            PropexPath::Multiple(vec![
                Property(Cow::Borrowed("a")),
                Nested(vec![Property(Cow::Borrowed("msg")), Property(Cow::Borrowed("c")),]),
                Index(0),
                Property(Cow::Borrowed("fred"))
            ]),
            r#"pass a[msg.c][0]["fred"]"#
        );

        // Failures:
        assert!(parse(r#"a'b'.c"#).is_err(), r#"fail a'b'.c"#);
        assert!(parse(r#"a['b'.c"#).is_err(), r#"fail a['b'.c"#);
        assert!(parse(r#"a[]"#).is_err(), r#"fail a[]"#);
        assert!(parse(r#"a]"#).is_err(), r#"fail a]"#);
        assert!(parse(r#"a["#).is_err(), r#"fail a["#);
        assert!(parse(r#"a[0d]"#).is_err(), r#"fail a[0d]"#);
        assert!(parse(r#"a['"#).is_err(), r#"fail a['"#);
        assert!(parse(r#"a[']"#).is_err(), r#"fail a[']"#);
        assert!(parse(r#"a[0']"#).is_err(), r#"fail a[0']"#);
        assert!(parse(r#"a.[0]"#).is_err(), r#"fail a.[0]"#);
        assert!(parse(r#"[0]"#).is_err(), r#"fail [0]"#);
        assert!(parse(r#"a[0"#).is_err(), r#"fail a[0"#);
        assert!(parse(r#"a."#).is_err(), r#"fail a."#);
        assert!(parse(r#".a"#).is_err(), r#"fail .a"#);
        assert!(parse(r#"a. b"#).is_err(), r#"fail `a. b`"#);
        assert!(parse(r#" a.b"#).is_err(), r#"fail ` a.b`"#);
        assert!(parse(r#"a[0].[1]"#).is_err(), r#"fail `a[0].[1]`"#);
        assert!(parse(r#"a['']"#).is_err(), r#"fail `a['']`"#);
        assert!(parse(r#"'a.b'c"#).is_err(), r#"fail `'a.b'c`"#);
        assert!(parse("").is_err(), r#"fail <blank>"#);
        assert!(parse("a[b]").is_err(), r#"fail `a[b]`"#);
        assert!(parse("a[msg.]").is_err(), r#"fail `a[msg.]`"#);
        assert!(parse("a[msg[]").is_err(), r#"fail `a[msg[]`"#);
        assert!(parse("a[msg.[]]").is_err(), r#"fail `a[msg.[]]`"#);
        assert!(parse("a[msg['af]]").is_err(), r#"fail `a[msg['af]]`"#);
    }
}
