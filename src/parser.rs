use nom::{
    branch::alt,
    bytes::complete::{tag, take_until},
    combinator::{map, map_res, opt},
    multi::many_m_n,
    sequence::{delimited, preceded, terminated},
    IResult,
};
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Error {
    title: String,
    message: String,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum BulkString {
    String(String),
    Empty,
    Null,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Array {
    Items(Vec<Value>),
    Empty,
    Null,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    String(String),
    BulkString(BulkString),
    Error(Error),
    Int(isize),
    Array(Array),
    #[non_exhaustive]
    Unsupported,
}

fn simple_str(input: &str) -> IResult<&str, Value> {
    map(
        terminated(preceded(tag("+"), take_until("\r\n")), tag("\r\n")),
        |res: &str| Value::String(res.to_string()),
    )(input)
}

fn bulk_str(input: &str) -> IResult<&str, Value> {
    let head = map_res(
        delimited(tag("$"), take_until("\r\n"), tag("\r\n")),
        |res: &str| res.parse::<isize>(),
    )(input);

    let body = opt(terminated(take_until("\r\n"), tag("\r\n")));
    head.and_then(|(next, size)| {
        if size == -1 {
            return Ok((next, Value::BulkString(BulkString::Null)));
        }
        map(body, |res: Option<&str>| match size {
            0 => Value::BulkString(BulkString::Empty),
            _ => Value::BulkString(BulkString::String(res.unwrap_or_default().to_owned())),
        })(next)
    })
}

fn int(input: &str) -> IResult<&str, Value> {
    map_res(
        terminated(preceded(tag(":"), take_until("\r\n")), tag("\r\n")),
        |res: &str| {
            if res.starts_with("+") {
                res[1..].parse::<isize>().map(Value::Int)
            } else {
                res.parse::<isize>().map(Value::Int)
            }
        },
    )(input)
}

fn arr(input: &str) -> IResult<&str, Value> {
    let head = map_res(
        delimited(tag("*"), take_until("\r\n"), tag("\r\n")),
        |res: &str| res.parse::<isize>(),
    )(input);

    head.and_then(|(next, size)| {
        map(
            opt(many_m_n(
                size as usize,
                size as usize,
                alt((simple_str, int, bulk_str, error, arr)),
            )),
            |res: Option<Vec<Value>>| match size {
                -1 => Value::Array(Array::Null),
                0 => Value::Array(Array::Empty),
                _ => Value::Array(Array::Items(res.unwrap_or_default())),
            },
        )(next)
    })
}

pub fn parser(input: &str) -> IResult<&str, Value> {
    alt((simple_str, int, bulk_str, error, arr))(input)
}

fn error(input: &str) -> IResult<&str, Value> {
    let pattern = delimited(tag("-"), take_until("\r\n"), tag("\r\n"));
    map(pattern, |res: &str| {
        let entry = res
            .split_once(" ")
            .ok_or(format!("invalid error message {res}"));
        match entry {
            Ok((title, message)) => Value::Error(Error {
                title: title.to_string(),
                message: message.to_string(),
            }),
            Err(_) => Value::Error(Error {
                title: res.to_string(),
                message: "".to_string(),
            }),
        }
    })(input)
}

impl From<&str> for BulkString {
    fn from(value: &str) -> Self {
        BulkString::String(value.to_string())
    }
}

impl From<BulkString> for Value {
    fn from(value: BulkString) -> Self {
        Value::BulkString(value)
    }
}

impl From<Array> for Value {
    fn from(value: Array) -> Self {
        Value::Array(value)
    }
}

impl BulkString {
    pub fn inner(&self) -> String {
        match self {
            BulkString::String(inner) => inner.to_string(),
            BulkString::Empty => "".to_string(),
            BulkString::Null => "".to_string(),
        }
    }
}

impl ToString for BulkString {
    fn to_string(&self) -> String {
        match self {
            BulkString::String(inner) => format!("${}\r\n{}\r\n", inner.len(), inner),
            BulkString::Empty => "$0\r\n\r\n".to_string(),
            BulkString::Null => "$-1\r\n".to_string(),
        }
    }
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::String(entry) => format!("+{entry}\r\n"),
            b @ Value::BulkString(_) => b.to_string(),
            Value::Error(err) => format!(
                "-{}{}{}",
                err.title,
                if err.message.is_empty() { "" } else { " " },
                err.message
            ),
            Value::Int(int) => format!(":{}\r\n", int.to_string()),
            a @ Value::Array(..) => a.to_string(),
            Value::Unsupported => unimplemented!("Unsupported"),
        }
    }
}

impl ToString for Array {
    fn to_string(&self) -> String {
        match self {
            Array::Items(arr) => {
                format!(
                    "*{}\r\n{}",
                    arr.len(),
                    arr.iter().map(ToString::to_string).collect::<String>()
                )
            }
            Array::Empty => "*0\r\n".to_string(),
            Array::Null => "*-1\r\n".to_string(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn simple_str_works() {
        let (remaining, value) = simple_str("+OK\r\n").unwrap();
        assert_eq!(value, Value::String("OK".into()).into());
        assert_eq!(remaining, "");
    }

    #[test]
    fn error_works() {
        let errors = [
            "-Error message\r\n",
            "-ERR unknown command 'asdf'\r\n",
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            "-World\r\n",
        ];

        for err in errors {
            let (remaining, value) = error(err).unwrap();
            assert!(matches!(value, Value::Error(Error { .. })));
            assert_eq!(remaining, "");
        }
    }

    #[test]
    fn int_works() {
        let ints = [":10\r\n", ":-1000\r\n", ":+2000\r\n"];

        for it in ints {
            let (remaining, value) = int(it).unwrap();
            assert!(matches!(value, Value::Int(..)));
            assert_eq!(remaining, "");
        }
    }

    #[test]
    fn bulk_str_works() {
        let strs = ["$5\r\nhello\r\n", "$0\r\n\r\n", "$-1\r\n"];

        for s in strs {
            let (remaining, value) = bulk_str(s).unwrap();
            assert!(matches!(value, Value::BulkString(..)));
            assert_eq!(remaining, "");
        }
    }

    #[test]
    fn arr_works() {
        let arrays = [
            ("*0\r\n", Array::Empty),
            (
                "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
                Array::Items(vec![
                    BulkString::from("hello").into(),
                    BulkString::from("world").into(),
                ]),
            ),
            (
                "*3\r\n:1\r\n:2\r\n:3\r\n",
                Array::Items(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            ),
            (
                "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n",
                Array::Items(vec![
                    Value::Int(1),
                    Value::Int(2),
                    Value::Int(3),
                    Value::Int(4),
                    BulkString::from("hello").into(),
                ]),
            ),
            (
                "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n",
                Array::Items(vec![
                    Array::Items(vec![Value::Int(1), Value::Int(2), Value::Int(3)]).into(),
                    Array::Items(vec![
                        Value::String("Hello".to_string()),
                        Value::Error(Error {
                            title: "World".to_string(),
                            message: "".to_string(),
                        }),
                    ])
                    .into(),
                ]),
            ),
            ("*-1\r\n", Array::Null),
            (
                "*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n",
                Array::Items(vec![
                    BulkString::from("hello").into(),
                    BulkString::Null.into(),
                    BulkString::from("world").into(),
                ]),
            ),
        ];

        for (input, expected) in arrays {
            let (remaining, value) = arr(input).unwrap();
            assert_eq!(value, expected.into());
            assert_eq!(remaining, "");
        }
    }
}
