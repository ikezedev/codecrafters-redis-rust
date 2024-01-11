mod config;
mod message;
mod parser;

use std::{
    collections::HashMap,
    error::Error,
    fs::File,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, OnceLock},
    thread,
    time::{Duration, Instant},
};

use config::Config;
use message::RespMessage;
use parser::{
    rdb::RDB,
    resp::{parser, Value},
};
use thiserror::Error;

use crate::parser::resp::BulkString;
use crate::parser::{rdb::parse_rdb, resp::Array};

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

static CONFIG: OnceLock<Config> = OnceLock::new();

fn main() -> Result<(), Box<dyn Error>> {
    CONFIG.set(Config::new()).unwrap();

    let rdb = if let Some(filename) = CONFIG
        .get()
        .and_then(|c| c.dir_to_path().zip(c.filename()))
        .map(|(dir, name)| dir.join(name))
    {
        if filename.exists() {
            let mut file = File::open(filename)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;
            match parse_rdb(&buffer) {
                Ok((_, rdb)) => Arc::new(Some(rdb)),
                Err(err) => {
                    eprintln!("{err}");
                    unreachable!()
                }
            }
        } else {
            Arc::new(None)
        }
    } else {
        Arc::new(None)
    };

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_requests(stream, Arc::clone(&rdb));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_requests(mut stream: TcpStream, rdb: Arc<Option<RDB>>) {
    // use crate::parser::resp::Value::*;

    let mut store = HashMap::<String, DurableValue>::new();
    thread::spawn(move || loop {
        let mut buffer = [0; 512];
        match stream.read(&mut buffer) {
            Ok(_) => {
                let entry = String::from_utf8(buffer.to_vec()).unwrap();

                let message: RespMessage = if let Ok((_, val)) = parser(&entry) {
                    val.try_into().unwrap()
                } else {
                    continue;
                };

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
                    RespMessage::ConfigGet(key) => match &key[..] {
                        "dir" => {
                            let _ = CONFIG.get().unwrap().dir_to_value().reply(&mut stream);
                        }
                        "dbfilename" => {
                            let _ = CONFIG.get().unwrap().filename_to_value().reply(&mut stream);
                        }
                        _ => {
                            eprintln!("unexpected config key: {key}");
                        }
                    },
                    RespMessage::Key(key) => {
                        eprintln!("key: {key}");
                        // let value: Value = if let Some(db) = rdb.as_ref() {
                        //     let items = db
                        //         .get(&key)
                        //         .into_iter()
                        //         .map(|v| Value::from(v))
                        //         .collect::<Vec<_>>();
                        //     Array::Items(items).into()
                        // } else {
                        //     Array::Empty.into()
                        // };
                        let value: Value = if let Some(db) = rdb.as_ref() {
                            let items = db.keys().map(|v| Value::from(v)).collect::<Vec<_>>();
                            Array::Items(items).into()
                        } else {
                            Array::Empty.into()
                        };

                        let _ = value.reply(&mut stream);
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
