/*!
Redis Serialization Protocol相关的解析代码
FROM: 
https://github.com/maplestoria/redis-event/blob/bb95b1c71d8517499f8efa8ca910d3b13fa0d3ea/src/lib.rs
*/
#![allow(dead_code)]
use std::io::{Read};
use anyhow::{Result, Ok,anyhow};

use byteorder::ReadBytesExt;

use crate::to_string;


pub(crate) const CR: u8 = b'\r';
pub(crate) const LF: u8 = b'\n';
pub(crate) const STAR: u8 = b'*';
pub(crate) const DOLLAR: u8 = b'$';
pub(crate) const PLUS: u8 = b'+';
pub(crate) const MINUS: u8 = b'-';
pub(crate) const COLON: u8 = b':';

pub trait RespDecode: Read {
    
    fn decode_resp(&mut self) -> Result<Resp> {
        match self.decode_type()? {
            Type::String => Ok(Resp::String(self.decode_string()?)),
            Type::Int => self.decode_int(),
            Type::Error => Ok(Resp::Error(self.decode_string()?)),
            Type::BulkString => self.decode_bulk_string(),
            Type::Array => self.decode_array(),
        }
    }
   
    fn decode_type(&mut self) -> Result<Type> {
        loop {
            let b = self.read_u8()?;
            if b == LF {
                continue;
            } else {
                match b {
                    PLUS => return Ok(Type::String),
                    MINUS => return Ok(Type::Error),
                    COLON => return Ok(Type::Int),
                    DOLLAR => return Ok(Type::BulkString),
                    STAR => return Ok(Type::Array),
                    _ => return  Err(anyhow!("decode_type err: {}",b)),
                }
            }
        }
    }
    
    fn decode_string(&mut self) -> Result<String> {
        let mut buf = vec![];
        loop {
            let byte = self.read_u8()?;
            if byte != CR {
                buf.push(byte);
            } else {
                break;
            }
        }
        if self.read_u8()? == LF {
            Ok(to_string(buf))
        } else {
            Err(anyhow!("Expect LF after CR"))
        }
    }

    
    fn decode_int(&mut self) -> Result<Resp> {
        let s = self.decode_string()?;
        let i = s.parse::<i64>().unwrap();
        Ok(Resp::Int(i))
    }


    fn decode_bulk_string(&mut self) -> Result<Resp> {
        let r = self.decode_int()?;
        if let Resp::Int(i) = r {
            if i > 0 {
                let mut buf = vec![0; i as usize];
                self.read_exact(&mut buf)?;
                let mut end = vec![0; 2];
                self.read_exact(&mut end)?;
                if !end.eq(&[CR, LF]) {
                    Err(anyhow!("Expected CRLF"))
                } else {
                    Ok(Resp::BulkBytes(buf))
                }
            } else {
                self.read_exact(&mut [0; 2])?;
                Ok(Resp::BulkBytes(vec![0; 0]))
            }
        } else {
            Err(anyhow!("Expected Int Response"))
        }
    }

    /// 解析Array响应
    fn decode_array(&mut self) -> Result<Resp> {
        let r = self.decode_int()?;
        if let Resp::Int(i) = r {
            let mut arr = Vec::with_capacity(i as usize);
            for _ in 0..i {
                let resp = self.decode_resp()?;
                arr.push(resp);
            }
            Ok(Resp::Array(arr))
        } else {
            Err(anyhow!("Expected Int Response"))
        }
    }
}

impl<R: Read + ?Sized> RespDecode for R {}

pub enum Type {
    String,
    Error,
    Int,
    BulkString,
    Array,
}

#[derive(Debug,PartialEq)]
pub enum Resp {
    String(String),
    Int(i64),
    Error(String),
    BulkBytes(Vec<u8>),
    Array(Vec<Resp>),
}




#[cfg(test)]
mod test {
    use crate::resp::{Resp, RespDecode};
    use std::io::Cursor;

    #[test]
    fn test_decode_array() {
        let b = b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n";
        let mut cursor = Cursor::new(b);
        let r = cursor.decode_resp();
        assert!(r.is_ok());
        match r {
            Ok(resp) => match resp {
                Resp::Array(arr) => {
                    let mut data = Vec::new();
                    for x in arr {
                        match x {
                            Resp::BulkBytes(bytes) => data.push(bytes),
                            _ => eprintln!("wrong type"),
                        }
                    }
                    assert!(b"SELECT".eq(data.get(0).unwrap().as_slice()));
                    assert!(b"0".eq(data.get(1).unwrap().as_slice()));
                }
                _ => eprintln!("wrong type"),
            },
            Err(err) => eprintln!("decode_resp err,{}",err),
        }
    }
}