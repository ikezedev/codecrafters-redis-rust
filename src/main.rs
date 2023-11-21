// Uncomment this block to pass the first stage
use std::{
    io::{self, Write},
    net::{TcpListener, TcpStream},
};

use thiserror::Error;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
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
    let _ = stream.write(b"+PONG\r\n");
}

#[derive(Error, Debug)]
enum RedisError {
    #[error("could not read stream")]
    ReadStream(#[from] io::Error),
}
