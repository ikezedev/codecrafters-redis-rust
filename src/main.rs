mod message;
mod parser;

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::{Duration, Instant},
};

use message::RespMessage;
use parser::{parser, Value};
use thiserror::Error;

use crate::parser::BulkString;

#[derive(Debug, Clone, PartialEq)]
struct DurableValue {
    val: Value,
    timing: Option<ValueTime>,
}

#[derive(Debug, Clone, PartialEq)]
struct ValueTime {
    duration: Duration,
    insert_at: Instant,
}

impl DurableValue {
    pub fn reply(&self, stream: &mut TcpStream) -> io::Result<usize> {
        stream.write(self.val.to_string().as_bytes())
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_requests(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_requests(mut stream: TcpStream) {
    let mut store = HashMap::<String, DurableValue>::new();
    thread::spawn(move || loop {
        let mut buffer = [0; 512];
        match stream.read(&mut buffer) {
            Ok(_) => {
                let entry = String::from_utf8(buffer.to_vec()).unwrap();

                dbg!(&entry);
                let message: RespMessage = if let Ok((_, val)) = parser(&entry) {
                    val.try_into().unwrap()
                } else {
                    continue;
                };
                dbg!(&message);

                match message {
                    RespMessage::Ping => {
                        let _ = stream.write(Value::String("PONG".into()).to_string().as_bytes());
                    }
                    RespMessage::Echo(bs) => {
                        let _ = stream.write(bs.to_string().as_bytes());
                    }
                    RespMessage::Set { key, val, expiry } => {
                        if let Some(millis) = expiry {
                            store.insert(
                                key,
                                DurableValue {
                                    val,
                                    timing: Some(ValueTime {
                                        duration: Duration::from_millis(millis as u64),
                                        insert_at: Instant::now(),
                                    }),
                                },
                            );
                        } else {
                            store.insert(key, DurableValue { val, timing: None });
                        }
                        let _ = Value::String("OK".into()).reply(&mut stream);
                    }
                    RespMessage::Get(key) => {
                        let val = store.get(&key).unwrap_or(&DurableValue {
                            val: Value::BulkString(BulkString::Null),
                            timing: None,
                        });
                        if let Some(timing) = &val.timing {
                            if timing.insert_at.elapsed() > timing.duration {
                                store.remove(&key);
                                let _ = Value::BulkString(BulkString::Null).reply(&mut stream);
                            } else {
                                let _ = val.reply(&mut stream);
                            }
                        } else {
                            let _ = val.reply(&mut stream);
                        }
                    }
                }
            }
            Err(_) => {
                break;
            }
        }
    });
}

#[derive(Error, Debug)]
enum RedisError {
    #[error("could not read stream")]
    ReadStream(#[from] io::Error),
}
