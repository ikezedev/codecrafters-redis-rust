mod message;
mod parser;

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use message::RespMessage;
use parser::{parser, Value};
use thiserror::Error;

use crate::parser::BulkString;

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
    let mut store = HashMap::<String, Value>::new();
    thread::spawn(move || loop {
        let mut buffer = [0; 512];
        match stream.read(&mut buffer) {
            Ok(_) => {
                let entry = String::from_utf8(buffer.to_vec()).unwrap();

                // dbg!(&entry);
                let message: RespMessage = if let Ok((_, val)) = parser(&entry) {
                    val.try_into().unwrap()
                } else {
                    continue;
                };
                // dbg!(&message);

                match message {
                    RespMessage::Ping => {
                        let _ = stream.write(Value::String("PONG".into()).to_string().as_bytes());
                    }
                    RespMessage::Echo(bs) => {
                        let _ = stream.write(bs.to_string().as_bytes());
                    }
                    RespMessage::Set { key, val } => {
                        store.insert(key, val);
                        let _ = stream.write(Value::String("OK".into()).to_string().as_bytes());
                    }
                    RespMessage::Get(key) => {
                        let val = store
                            .get(&key)
                            .unwrap_or(&Value::BulkString(BulkString::Null));
                        dbg!(val);
                        let ret = val.to_string();
                        dbg!(&ret);
                        let _ = stream.write(ret.as_bytes());
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
