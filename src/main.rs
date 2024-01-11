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
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use config::Config;
use message::RespMessage;
use parser::{
    rdb::KVPair,
    resp::{parser, Value},
};
use thiserror::Error;

use crate::parser::resp::BulkString;
use crate::parser::{rdb::parse_rdb, resp::Array};

#[derive(Debug, Clone, PartialEq)]
struct DurableValue {
    val: Value,
    expiration: Expiration,
}

#[derive(Debug, Clone, PartialEq, Default)]
enum Expiration {
    #[default]
    Empty,
    Date(SystemTime),
    Period {
        duration: Duration,
        insert_at: Instant,
    },
}

impl Expiration {
    fn elapsed(&self) -> bool {
        match self {
            Expiration::Empty => false,
            Expiration::Date(time) => SystemTime::now() >= *time,
            Expiration::Period {
                duration,
                insert_at,
            } => insert_at.elapsed() > *duration,
        }
    }
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

            let (_, rdb) = parse_rdb(&buffer).map_err(|err| format!("{err}"))?;
            let map = rdb
                .entries()
                .map(
                    |KVPair {
                         key,
                         value,
                         expiration,
                     }| {
                        (
                            key.to_string(),
                            DurableValue {
                                val: Value::from(value),
                                expiration: expiration
                                    .map(|exp| Expiration::Date(UNIX_EPOCH + exp))
                                    .unwrap_or_default(),
                            },
                        )
                    },
                )
                .collect::<HashMap<_, _>>();
            Arc::new(map)
        } else {
            Arc::new(HashMap::default())
        }
    } else {
        Arc::new(HashMap::default())
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

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time cannot go before 1970 with this implementation")
        .as_millis() as u64
}

fn handle_requests(mut stream: TcpStream, rdb: Arc<HashMap<String, DurableValue>>) {
    let mut store: HashMap<String, DurableValue> =
        HashMap::from_iter(rdb.iter().map(|(k, v)| (k.clone(), v.clone())));

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
                                    expiration: Expiration::Period {
                                        duration: Duration::from_millis(millis as u64),
                                        insert_at: Instant::now(),
                                    },
                                },
                            );
                        } else {
                            store.insert(
                                key,
                                DurableValue {
                                    val,
                                    expiration: Expiration::Empty,
                                },
                            );
                        }
                        let _ = Value::String("OK".into()).reply(&mut stream);
                    }
                    RespMessage::Get(key) => {
                        let val = store.get(&key).unwrap_or(&DurableValue {
                            val: Value::BulkString(BulkString::Null),
                            expiration: Expiration::Empty,
                        });

                        if val.expiration.elapsed() {
                            store.remove(&key);
                            let _ = Value::BulkString(BulkString::Null).reply(&mut stream);
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
                    RespMessage::Keys(_) => {
                        let keys = store
                            .keys()
                            .map(|k| BulkString::String(k.to_string()).into())
                            .collect();
                        let value: Value = Array::Items(keys).into();

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
