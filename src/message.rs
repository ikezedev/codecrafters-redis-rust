use crate::parser::{Array, BulkString, Value};

pub enum RespMessage {
    Ping,
    Echo(BulkString),
    Set { key: String, val: Value },
    Get(String),
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
                [Value::BulkString(set), Value::BulkString(key), val]
                    if set.inner().to_lowercase() == "set" =>
                {
                    Ok(RespMessage::Set {
                        key: key.inner(),
                        val: val.clone(),
                    })
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
