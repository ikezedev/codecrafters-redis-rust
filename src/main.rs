mod message;
mod parser;

use std::{
    fmt::format,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use message::RespMessage;
use parser::parser;
use thiserror::Error;

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
    thread::spawn(move || loop {
        let mut buffer = [0; 512];
        match stream.read(&mut buffer) {
            Ok(_) => {
                let entry = String::from_utf8(buffer.to_vec()).unwrap();

                let message: RespMessage = parser(&entry).unwrap().1.try_into().unwrap();
                dbg!(&message);

                match message {
                    RespMessage::Ping => {
                        let _ = stream.write(b"+PONG\r\n");
                    }
                    RespMessage::Pong => {}
                    RespMessage::Echo(bs) => {
                        let _ = stream.write(bs.to_string().as_bytes());
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
