use crate::parser::{Array, BulkString, Value};

#[derive(Debug)]
pub enum RespMessage {
    Ping,
    Pong,
    Echo(BulkString),
}

impl TryFrom<Value> for RespMessage {
    type Error = (String, Value);

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match &value {
            Value::Array(Array::Items(entry)) => match entry.as_slice() {
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
