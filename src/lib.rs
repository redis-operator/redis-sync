#![allow(dead_code)]
#![allow(clippy::upper_case_acronyms)]
#![feature(read_buf)]
use std::io;
use std::io::Read;

use std::io::Result;


mod config;
mod error;
mod resp;
mod connect;
mod rdb;
mod cmd;
mod iter;
mod lzf;
use crate::rdb::{Module, Object};
use crate::cmd::Command;

pub trait RedisListener {
    /// 开启事件监听
    fn start(&mut self) -> Result<()>;
}

#[derive(Debug)]
pub enum Event<'a> {
    RDB(Object<'a>),
    AOF(Command<'a>),
}

pub trait EventHandler {
    fn handle(&mut self, event: Event);
}


pub struct NoOpEventHandler {}

impl EventHandler for NoOpEventHandler {
    fn handle(&mut self, _: Event) {}
}


// pub trait RDBParser :Read {
//     fn parse(&mut self, event_handler: &mut dyn EventHandler) -> Result<()>;
// }


pub trait ModuleParser {
    /// 解析Module的具体实现
    ///
    /// 方法参数:
    ///
    /// * `input`: RDB输入流
    /// * `module_name`: Module的名字
    /// * `module_version`: Module的版本
    fn parse(&mut self, input: &mut dyn Read, module_name: &str, module_version: usize) -> Box<dyn Module>;
}

#[allow(dead_code)]
fn to_string(bytes: Vec<u8>) -> String {
    return  unsafe {
        std::str::from_utf8_unchecked(&bytes).to_string()
    };
}

pub(crate) fn skip(input: &mut dyn Read, length: u64) -> Result<()> {
// used for diskless transfers
// when the master does not know beforehand the size of the file to
// transfer. In the latter case, the following format is used:
// $EOF:<40 bytes delimiter>
    io::copy(&mut input.take(length), &mut io::sink())?;
    Ok(())
}