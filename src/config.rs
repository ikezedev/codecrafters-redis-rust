use std::path::PathBuf;

use itertools::Itertools;

use crate::parser::resp::{Array, BulkString, Value};

#[derive(Default, Debug)]
pub struct Config {
    dir: Option<String>,
    filename: Option<String>,
}

impl FromIterator<(String, String)> for Config {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        let mut config = Config::default();
        for (key, value) in iter {
            if config.filename.is_some() && config.dir.is_some() {
                break;
            }
            match &key[..] {
                "--dir" => {
                    config.dir = Some(value);
                }
                "--dbfilename" => {
                    config.filename = Some(value);
                }
                _ => (),
            }
        }
        config
    }
}

impl Config {
    pub fn new() -> Self {
        std::env::args().skip(1).tuple_windows().collect::<Self>()
    }
    #[allow(dead_code)]
    pub fn dir_to_path(&self) -> Option<PathBuf> {
        self.dir.as_ref().map(PathBuf::from)
    }

    pub fn dir_to_value(&self) -> Value {
        self.dir
            .as_ref()
            .map(|dir| {
                Array::Items(vec![
                    BulkString::String("dir".to_string()).into(),
                    BulkString::String(dir.to_string()).into(),
                ])
                .into()
            })
            .unwrap_or(Array::Empty.into())
    }

    pub fn filename_to_value(&self) -> Value {
        self.dir
            .as_ref()
            .map(|dir| {
                Array::Items(vec![
                    BulkString::String("dbfilename".to_string()).into(),
                    BulkString::String(dir.to_string()).into(),
                ])
                .into()
            })
            .unwrap_or(Array::Empty.into())
    }

    #[allow(dead_code)]
    pub fn filename(&self) -> Option<String> {
        self.filename.as_ref().map(ToString::to_string)
    }
}
