use crate::parser::resp::{Array, BulkString, Value};

#[derive(Debug)]
pub enum RespMessage {
    Ping,
    Echo(BulkString),
    Set {
        key: String,
        val: Value,
        expiry: Option<usize>,
    },
    Get(String),
    ConfigGet(String),
    Key(String),
}

impl TryFrom<Value> for RespMessage {
    type Error = (String, Value);

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match &value {
            Value::Array(Array::Items(entry)) => match entry.as_slice() {
                [Value::BulkString(get), Value::BulkString(key)]
                    if get.inner().to_lowercase() == "get" =>
                {
                    Ok(RespMessage::Get(key.inner()))
                }
                [Value::BulkString(key), Value::BulkString(key_value)]
                    if key.inner().to_lowercase() == "key" =>
                {
                    Ok(RespMessage::Key(key_value.inner()))
                }
                [Value::BulkString(config), Value::BulkString(get), Value::BulkString(key)]
                    if config.inner().to_lowercase() == "config"
                        && get.inner().to_lowercase() == "get" =>
                {
                    Ok(RespMessage::ConfigGet(key.inner()))
                }
                [Value::BulkString(set), Value::BulkString(key), val, rest @ ..]
                    if set.inner().to_lowercase() == "set" =>
                {
                    match rest {
                        [Value::BulkString(px), Value::BulkString(millis), ..]
                            if px.inner().to_lowercase() == "px" =>
                        {
                            Ok(RespMessage::Set {
                                key: key.inner(),
                                val: val.clone(),
                                expiry: Some(
                                    millis
                                        .inner()
                                        .parse::<usize>()
                                        .expect("could not parse expiry duration"),
                                ),
                            })
                        }
                        _ => Ok(RespMessage::Set {
                            key: key.inner(),
                            val: val.clone(),
                            expiry: None,
                        }),
                    }
                }
                [Value::BulkString(fs), Value::BulkString(sec)]
                    if fs.inner().to_lowercase() == "echo" =>
                {
                    Ok(RespMessage::Echo(sec.clone()))
                }
                [Value::BulkString(fs)] if fs.inner().to_lowercase() == "ping" => {
                    Ok(RespMessage::Ping)
                }
                _ => Err(("Unsupported".to_string(), value)),
            },
            _ => Err(("Unsupported".to_string(), value)),
        }
    }
}
