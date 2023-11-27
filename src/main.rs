use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

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
                let _ = stream.write(b"+PONG\r\n");
            }
            Err(_) => {
                break;
            }
        }
    });
    // handle.join().expect("thread could not be joined");
}

#[derive(Error, Debug)]
enum RedisError {
    #[error("could not read stream")]
    ReadStream(#[from] io::Error),
}
