use nom::branch::alt;
use nom::bytes::complete::{tag, take};
use nom::combinator::{map, map_res, opt};

use nom::error::{ErrorKind, FromExternalError};
use nom::multi::{many0, many_till};
use nom::number::complete::{be_i16, be_i32, be_i8, be_u32, be_u64, be_u8};
use nom::sequence::{pair, preceded};
use nom::{IResult as NomResult, Parser};

const MAGIC: &[u8; 5] = b"REDIS";

trait ParseRDB<'a, T>: Parser<&'a [u8], T, nom::error::Error<&'a [u8]>> {}
type IResult<'a, T> = NomResult<&'a [u8], T>;

impl<'a, T, U> ParseRDB<'a, T> for U where U: Parser<&'a [u8], T, nom::error::Error<&'a [u8]>> {}

#[derive(Debug, PartialEq)]
pub struct RDB {
    pub version: u32,
    pub auxilliary_field: Vec<Auxilliary>,
    pub databases: Vec<DB>,
}
#[derive(Debug, PartialEq)]
pub struct Auxilliary {
    key: DBString,
    value: DBString,
}

#[derive(PartialEq, Debug)]
enum LenEncoded {
    Num(u32),
    Special(u8),
}

#[derive(PartialEq, Debug)]
pub struct KVPair {
    pub key: DBString,
    pub value: Value,
    pub expiration: Option<u64>,
}

#[derive(PartialEq, Debug)]
pub struct DB {
    pub number: u32,
    pub resize_db: Option<ResizeDBAttr>,
    pub key_value_pairs: Vec<KVPair>,
}

impl RDB {
    #[allow(dead_code)]
    pub fn get<'a, 'b: 'a>(&'a self, key: &'b str) -> impl Iterator<Item = &'a Value> + 'a {
        self.databases.iter().flat_map(|db| db.get(key))
    }

    #[allow(dead_code)]
    pub fn keys<'a>(&'a self) -> impl Iterator<Item = &'a DBString> + 'a {
        self.databases.iter().flat_map(|db| db.keys())
    }
}

impl DB {
    #[allow(dead_code)]
    fn get<'a, 'b>(&'a self, key: &'b str) -> Option<&'a Value> {
        self.key_value_pairs
            .iter()
            .find(|KVPair { key: k, .. }| k.to_string() == key)
            .map(|k| &k.value)
    }

    pub fn keys<'a>(&'a self) -> impl Iterator<Item = &'a DBString> + 'a {
        self.key_value_pairs.iter().map(|kv| &kv.key)
    }
}

#[derive(PartialEq, Debug)]
pub struct ResizeDBAttr {
    pub hash_table_size: u32,
    pub expire_hash_table_size: u32,
}

#[derive(PartialEq, Debug)]
pub enum DBString {
    Int(i32),
    Str(String),
    #[allow(dead_code)]
    Lzf {
        clen: u32,
        ulen: u32,
        data: Vec<u8>,
    },
}

impl ToString for DBString {
    fn to_string(&self) -> String {
        match self {
            DBString::Int(ref num) => num.to_string(),
            DBString::Str(s) => s.to_owned(),
            DBString::Lzf { .. } => String::new(),
        }
    }
}

impl PartialEq<str> for Value {
    fn eq(&self, other: &str) -> bool {
        match self {
            Value::String(s) => s.to_string() == other,
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum Value {
    String(DBString),
}

impl From<&Value> for super::resp::Value {
    fn from(value: &Value) -> Self {
        match value {
            Value::String(s) => s.into(),
        }
    }
}

impl From<Value> for super::resp::Value {
    fn from(value: Value) -> Self {
        (&value).into()
    }
}

impl From<&DBString> for super::resp::Value {
    fn from(value: &DBString) -> Self {
        Self::String(value.to_string())
    }
}

fn nom_error<'a, T>(input: &'a [u8], msg: impl Into<String>) -> IResult<'a, T> {
    Err(nom::Err::Error(nom::error::Error::from_external_error(
        input,
        ErrorKind::MapRes,
        msg.into(),
    )))
}

pub fn parse_rdb<'a>(input: &'a [u8]) -> IResult<'a, RDB> {
    let (input, version) = header(input)?;
    let (input, auxilliary_field) = auxilliary(input)?;
    let (input, (databases, _)) = many_till(db, tag([0xff]))(input)?;

    let res = RDB {
        version,
        databases,
        auxilliary_field,
    };
    return Ok((input, res));
}

fn header(input: &[u8]) -> IResult<u32> {
    preceded(
        tag(MAGIC),
        map_res(map_res(take(4u8), std::str::from_utf8), str::parse::<u32>),
    )(input)
}

fn len(input: &[u8]) -> IResult<LenEncoded> {
    let (next, l) = be_u8(input)?;
    match l >> 6 {
        0b00 => Ok((next, LenEncoded::Num((l & 0x3F).into()))), // The next 6 bits represent the length

        // Read one additional byte. The combined 14 bits represent the length
        0b01 => map(be_u8, |ex| {
            LenEncoded::Num(((l as u32 & 0x3F) << 8) | (ex as u32))
        })(next),

        // Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
        0b10 => map(be_u32, |n| LenEncoded::Num(n))(next),

        // The next object is encoded in a special format. The remaining 6 bits indicate the format.
        0b11 => Ok((next, LenEncoded::Special(l & 0x3F))),
        other => nom_error(next, format!("Invalid length encoded value: {other:b}")),
    }
}

fn string(input: &[u8]) -> IResult<DBString> {
    let (next, len) = len(input)?;
    match len {
        LenEncoded::Num(num) => map_res(take(num), |bytes| {
            std::str::from_utf8(bytes).map(|s| DBString::Str(s.into()))
        })(next),
        LenEncoded::Special(flag) => match flag {
            0 => map(be_i8, |n| DBString::Int(n as i32))(next),
            1 => map(be_i16, |n| DBString::Int(n as i32))(next),
            2 => map(be_i32, |n| DBString::Int(n))(next),
            3 => nom_error(next, "Lzf strings are not supported yet"),
            // 3 => Ok((next, DBString::Lzf)),
            _ => nom_error(next, "Unspported special length encoding"),
        },
    }
}

fn kv_pair(input: &[u8]) -> IResult<KVPair> {
    let (input, variant) = opt(alt((tag([0xFD]), tag([0xFC]))))(input)?;

    let (input, expiration) = match variant {
        Some([0xFD]) => map(be_u32, |n| Some(n.into()))(input)?,
        Some([0xFC]) => map(be_u64, Some)(input)?,
        _ => (input, None),
    };

    let (input, value_type) = be_u8(input)?;
    let (input, key) = string(input)?;

    let (input, value) = match value_type {
        0 => map(string, Value::String)(input),
        other => nom_error(input, format!("Unspported value type {other}")),
    }?;

    Ok((
        input,
        KVPair {
            key,
            value,
            expiration,
        },
    ))
}

fn map_len<'a>(
    mut parse_fn: impl ParseRDB<'a, LenEncoded>,
) -> impl FnMut(&'a [u8]) -> IResult<u32> {
    move |input: &'a [u8]| -> IResult<u32> {
        let (input, len) = parse_fn.parse(input)?;
        match len {
            LenEncoded::Num(num) => Ok((input, num)),
            other => nom_error(
                input,
                format!("expected LenEncoded::Num variant but got {other:?}"),
            ),
        }
    }
}

fn use_len(input: &[u8]) -> IResult<u32> {
    map_len(len)(input)
}

fn db_number(input: &[u8]) -> IResult<u32> {
    map_len(preceded(tag(&[0xFE]), len))(input)
}

fn resize_db(input: &[u8]) -> IResult<Option<ResizeDBAttr>> {
    opt(preceded(
        tag([0xFB]),
        map(pair(use_len, use_len), |(l1, l2)| ResizeDBAttr {
            hash_table_size: l1,
            expire_hash_table_size: l2,
        }),
    ))(input)
}

fn auxilliary(input: &[u8]) -> IResult<Vec<Auxilliary>> {
    let base = map(
        preceded(tag([0xFA]), pair(string, string)),
        |(key, value)| Auxilliary { key, value },
    );
    many0(base)(input)
}

fn db(input: &[u8]) -> IResult<DB> {
    let (input, number) = db_number(input)?;
    let (input, resize_db) = resize_db(input)?;
    let (_, (key_value_pairs, _)) = many_till(kv_pair, alt((tag([0xFE]), tag([0xFF]))))(input)?;

    // FIXME: feels like nom should save us from doing this, but wasn't able to find a solution from its docs

    let index = input
        .iter()
        .position(|&x| x == 0xFE_u8 || x == 0xFF_u8)
        .unwrap();
    Ok((
        &input[index..],
        DB {
            number,
            resize_db,
            key_value_pairs,
        },
    ))
}

#[cfg(test)]
mod test {
    use std::{error::Error, fs::File, io::Read};

    use super::*;
    #[test]
    fn parse_header() {
        let input: &[u8] = &[
            0x52, 0x45, 0x44, 0x49, 0x53, // magic
            0x30, 0x30, 0x30, 0x33, // version
        ];

        assert_eq!(header(input).unwrap().1, 3);
    }

    #[test]
    fn parse_len() -> Result<(), Box<dyn Error>> {
        let tests: &[(&[u8], LenEncoded)] = &[
            (
                &[0xC2, 0x25, 0xD3, 0xED, 0x52], // i32 string
                LenEncoded::Special(2),
            ),
            (
                &[0xC0, 0x7D], // i8 string
                LenEncoded::Special(0),
            ),
            (
                &[0xC1, 0xDB, 0x8C], // i16 string
                LenEncoded::Special(1),
            ),
            (&[0x3F], LenEncoded::Num(63)),
            (&[0x00], LenEncoded::Num(0)),
            (&[0x74, 0x69], LenEncoded::Num(13417)),
            (&[0x7F, 0xFF], LenEncoded::Num(16383)),
            (&[0xAB, 0xFF, 0xFF, 0xFF, 0xFF], LenEncoded::Num(4294967295)),
        ];

        for (input, expected) in tests {
            let (_, res) = len(input)?;
            assert_eq!(res, *expected);
        }
        Ok(())
    }

    #[test]
    fn parse_string() -> Result<(), Box<dyn Error>> {
        let tests: &[(&[u8], DBString)] = &[
            (
                &[0xC2, 0x25, 0xD3, 0xED, 0x52], // i32 string
                DBString::Int(634645842),
            ),
            (
                &[0xC0, 0x7D], // i8 string
                DBString::Int(125),
            ),
            (
                &[0xC0, 0x85], // i8 string
                DBString::Int(-123),
            ),
            (
                &[0xC1, 0xDB, 0x8C], // i16 string
                DBString::Int(-9332),
            ),
            (
                &[0xC2, 0xAB, 0xAB, 0x00, 0x00], // i32 string
                DBString::Int(-1414856704),
            ),
            (
                &[0xC2, 0xDB, 0x2C, 0x12, 0xF5], // i32 string
                DBString::Int(-617868555),
            ),
            (
                &[
                    0x17, 0x50, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x76, 0x65, 0x20, 0x33, 0x32, 0x20,
                    0x62, 0x69, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x65, 0x67, 0x65, 0x72,
                ],
                DBString::Str(String::from("Positive 32 bit integer")),
            ),
            (
                &[
                    0x16, 0x50, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x76, 0x65, 0x20, 0x38, 0x20, 0x62,
                    0x69, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x65, 0x67, 0x65, 0x72,
                ],
                DBString::Str(String::from("Positive 8 bit integer")),
            ),
        ];

        for (input, expected) in tests {
            let (next, res) = string(input)?;
            assert_eq!(res, *expected);
            assert_eq!(next.len(), 0);
        }
        Ok(())
    }

    #[test]
    fn parse_kv_pair() -> Result<(), Box<dyn Error>> {
        let tests: &[(&[u8], KVPair)] = &[
            (
                &[
                    0x00, 0xc2, 0x25, 0xd3, 0xed, 0x0a, 0x17, 0x50, 0x6f, 0x73, 0x69, 0x74, 0x69,
                    0x76, 0x65, 0x20, 0x33, 0x32, 0x20, 0x62, 0x69, 0x74, 0x20, 0x69, 0x6e, 0x74,
                    0x65, 0x67, 0x65, 0x72,
                ],
                KVPair {
                    value: Value::String(DBString::Str(String::from("Positive 32 bit integer"))),
                    expiration: None,
                    key: DBString::Int(634645770),
                },
            ),
            (
                &[
                    0x00, 0xc0, 0x7d, 0x16, 0x50, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x76, 0x65, 0x20,
                    0x38, 0x20, 0x62, 0x69, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x65, 0x67, 0x65, 0x72,
                ],
                KVPair {
                    value: Value::String(DBString::Str(String::from("Positive 8 bit integer"))),
                    expiration: None,
                    key: DBString::Int(125),
                },
            ),
        ];

        for (input, expected) in tests {
            let (next, res) = kv_pair(input)?;
            assert_eq!(res, *expected);
            assert_eq!(next.len(), 0);
        }
        Ok(())
    }

    #[test]
    fn parse_db() -> Result<(), Box<dyn Error>> {
        let input = &[
            0xfe, 0x00, 0x00, 0xc2, 0x25, 0xd3, 0xed, 0x0a, 0x17, 0x50, 0x6f, 0x73, 0x69, 0x74,
            0x69, 0x76, 0x65, 0x20, 0x33, 0x32, 0x20, 0x62, 0x69, 0x74, 0x20, 0x69, 0x6e, 0x74,
            0x65, 0x67, 0x65, 0x72, 0x00, 0xc0, 0x7d, 0x16, 0x50, 0x6f, 0x73, 0x69, 0x74, 0x69,
            0x76, 0x65, 0x20, 0x38, 0x20, 0x62, 0x69, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x65, 0x67,
            0x65, 0x72, 0x00, 0xc1, 0xdb, 0x8c, 0x17, 0x4e, 0x65, 0x67, 0x61, 0x74, 0x69, 0x76,
            0x65, 0x20, 0x31, 0x36, 0x20, 0x62, 0x69, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x65, 0x67,
            0x65, 0x72, 0x00, 0xc0, 0x85, 0x16, 0x4e, 0x65, 0x67, 0x61, 0x74, 0x69, 0x76, 0x65,
            0x20, 0x38, 0x20, 0x62, 0x69, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x65, 0x67, 0x65, 0x72,
            0x00, 0xc2, 0xab, 0xab, 0x00, 0x00, 0x17, 0x50, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x76,
            0x65, 0x20, 0x31, 0x36, 0x20, 0x62, 0x69, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x65, 0x67,
            0x65, 0x72, 0x00, 0xc2, 0xdb, 0x2c, 0x12, 0xf5, 0x17, 0x4e, 0x65, 0x67, 0x61, 0x74,
            0x69, 0x76, 0x65, 0x20, 0x33, 0x32, 0x20, 0x62, 0x69, 0x74, 0x20, 0x69, 0x6e, 0x74,
            0x65, 0x67, 0x65, 0x72, 0xff,
        ];

        let (input, db) = db(input)?;

        assert_eq!(input, &[0xff]);
        assert_eq!(db.number, 0);
        assert_eq!(db.resize_db, None);
        assert_eq!(db.key_value_pairs.len(), 6);

        Ok(())
    }

    #[test]
    fn parse_aux() -> Result<(), Box<dyn Error>> {
        let input = &[
            250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64, 250, 9, 114, 101,
            100, 105, 115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48,
        ];

        let (input, aux) = auxilliary(input)?;

        assert_eq!(input, &[]);
        assert_eq!(aux.len(), 2);
        dbg!(aux);

        Ok(())
    }
    #[test]
    fn db_number_parser() {
        let input: &[u8] = &[0xFE, 0x00];

        assert_eq!(db_number(input).unwrap().1, 0);
    }

    #[test]
    fn rdb() -> Result<(), Box<dyn Error>> {
        let mut file = File::open("./integer_keys.rdb").unwrap();
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)?;

        let (input, rdb) = parse_rdb(&buf).unwrap();
        assert_eq!(input, &[]);
        assert_eq!(rdb.version, 3);
        dbg!(rdb);
        Ok(())
    }

    #[test]
    fn rdb2() -> Result<(), Box<dyn Error>> {
        let input = &[
            82, 69, 68, 73, 83, 48, 48, 48, 51, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116,
            115, 192, 64, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114, 5, 55, 46, 50, 46,
            48, 254, 0, 251, 1, 0, 0, 4, 112, 101, 97, 114, 10, 115, 116, 114, 97, 119, 98, 101,
            114, 114, 121, 255, 255, 125, 246, 75, 211, 97, 140, 85, 10,
        ];

        let (input, rdb) = parse_rdb(input).unwrap();
        // assert_eq!(input, &[]);
        assert_eq!(rdb.version, 3);
        dbg!(rdb);
        Ok(())
    }
}
