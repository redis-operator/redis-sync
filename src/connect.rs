use anyhow::{anyhow, Result};

use log::{debug};
use redis::ToRedisArgs;
use std::io::{Read, Write};
use crate::rdb::RDBParser;
use crate::resp::{Resp, RespDecode, Type};

impl<R: Read + Write + ?Sized> Connect for R {}



pub trait Connect: Read + Write {
    fn auth(&mut self, password: Option<String>, username: Option<String>) -> Result<()> {
        let mut args = vec![];
        args.extend("AUTH".to_redis_args());
        if username.is_some() {
            args.extend(username.to_redis_args());
        }
        args.extend(password.to_redis_args());
        let result = redis::pack_command(&args);
        self.write_all(&result)?;
        let res = self.decode_resp()?;
        match res {
            Resp::String(r) => {
                if r == "OK" {
                    return Ok(());
                }
            }
            Resp::Error(e) => return Err(anyhow!("auth fail: {}", e)),
            _ => return Err(anyhow!("auth fail invalid err")),
        }
        Ok(())
    }

    fn ping(&mut self) -> Result<String> {
        let mut args = vec![];
        args.extend("PING".to_redis_args());
        let result = redis::pack_command(&args);
        self.write_all(&result)?;
        let res = self.decode_resp()?;
        match res {
            Resp::String(str) => {
                return Ok(str);
            }
            Resp::Error(err) => {
                if (err.contains("NOAUTH") || err.contains("NOPERM"))
                    && !err.contains("no password")
                    && !err.contains("Unrecognized REPLCONF option")
                {
                    return Err(anyhow!("ping  err: {:?}", err));
                }
            }
            _ => return Err(anyhow!("ping fail invalid err")),
        }
        Ok("".to_string())
    }

    fn reply(&mut self) -> Result<()> {
        match self.decode_resp()? {
            Resp::String(s) => {
                debug!("{:?}", s);
            }
            Resp::Error(err) => {
                if (err.contains("NOAUTH") || err.contains("NOPERM"))
                    && !err.contains("no password")
                    && !err.contains("Unrecognized REPLCONF option")
                {
                    return Err(anyhow!("reply  err: {:?}", err));
                }
            }
            _ => panic!("Unexpected response type"),
        }
        Ok(())
    }

    fn replconf(&mut self, ip: String, port: u16) -> Result<()> {
        let mut args = vec![];

        args.extend("REPLCONF".to_redis_args());
        args.extend("listening-port".to_redis_args());
        args.extend(port.to_redis_args());
        let result = redis::pack_command(&args);
        self.write_all(&result)?;
        self.reply()?;

        let mut args = vec![];
        args.extend("REPLCONF".to_redis_args());
        args.extend("ip-address".to_redis_args());
        args.extend(ip.to_redis_args());
        let result = redis::pack_command(&args);
        self.write_all(&result)?;
        self.reply()?;

        let mut args = vec![];
        args.extend("REPLCONF".to_redis_args());
        args.extend("capa".to_redis_args());
        args.extend("eof".to_redis_args());

        let result = redis::pack_command(&args);
        self.write_all(&result)?;
        self.reply()?;

        let mut args = vec![];
        args.extend("REPLCONF".to_redis_args());
        args.extend("capa".to_redis_args());
        args.extend("psync2".to_redis_args());

        let result = redis::pack_command(&args);
        self.write_all(&result)?;
        self.reply()
    }

    fn psync(
        &mut self,
        repl_id: String,
        repl_offset: String,
    ) -> Result<PsyncResp> {
        let mut args = vec![];

        args.extend("PSYNC".to_redis_args());
        args.extend(repl_id.to_redis_args());
        args.extend(repl_offset.to_redis_args());
        let result = redis::pack_command(&args);
        self.write_all(&result)?;

        match self.decode_resp() {
            Err(err) => {
                return Err( anyhow!(" decode_resp err:{:?} " ,err ));
            }
            Ok(response) => {
                if let Resp::String(resp) = &response {
                    println!("{:?}", resp);
                    let mut next_type = NextStep::ChangeMode;
                    let (mut repl_id, mut repl_offset, mut length) = ("".to_string(), 0i64, 0i64);
                    if resp.starts_with("FULLRESYNC") {
                        let mut iter = resp.split_whitespace();

                        if let Some(_repl_id) = iter.nth(1) {
                            repl_id = _repl_id.to_string();
                        } else {
                            return Err(anyhow!("Expect replication id, but got None"));
                        }
                        if let Some(_repl_offset) = iter.next() {
                            repl_offset = _repl_offset.parse::<i64>().expect("parse offset err");
                        } else {
                            return Err(anyhow!("Expect replication offset, but got None"));
                        }

                        if let Type::BulkString = self.decode_type()? {
                            let reply = self.decode_string()?;
                            if reply.starts_with("EOF") {
                                length = -1;
                            } else {
                                length = reply.parse::<i64>().expect("lenth parse err");
                            }
                        } else {
                            return Err(anyhow!("Expect BulkString response"));
                        }

                        next_type = NextStep::FullSync;
                    } else if resp.starts_with("CONTINUE") {
                        let mut iter = resp.split_whitespace();
                        if let Some(_repl_id) = iter.nth(1) {
                            if repl_id != _repl_id {
                                repl_id = _repl_id.to_owned();
                            }
                        }
                        next_type = NextStep::PartialResync;
                        length = -1;
                    } else if resp.starts_with("NOMASTERLINK")|| resp.starts_with("LOADING")  {
                        next_type = NextStep::Wait;
                        length = -1;
                    } ;
                    return Ok(PsyncResp { next_step:next_type, repl_id, repl_offset, length });
                }
            }
        }
        Ok(PsyncResp{next_step:NextStep::Wait,repl_id:"".to_string(),repl_offset:0,length:0})
        
    }
    

    fn replconf_ack(&mut self,repl_offset: String) -> Result<()> {
        let mut args = vec![];
        args.extend("PSYNC".to_redis_args());
        args.extend("ACK".to_redis_args());
        args.extend(repl_offset.to_redis_args());
        let result = redis::pack_command(&args);
        self.write_all(&result)?; 
        Ok(())
    }
    
     
}

#[warn(dead_code)]
#[derive(Debug, PartialEq)]
pub enum NextStep {
    FullSync,
    PartialResync,
    ChangeMode,
    Wait,
}

#[warn(dead_code)]
#[derive(Debug, PartialEq)]
pub struct PsyncResp{
   pub next_step: NextStep,
   pub repl_id: String,
   pub repl_offset: i64,
   pub length: i64,
}

#[cfg(test)]
mod test {
    use crate::{rdb::{RDBParser, RDBDecode}, resp::RespDecode};

    use super::Connect;
    use byteorder::ReadBytesExt;
    use redis::ToRedisArgs;
    use std::{net::TcpStream, sync::{atomic::AtomicBool, Arc}, io::{Write, Read, self}};
    use crate::{EventHandler,Event};

    pub struct PrintEventHandler {}

    impl EventHandler for PrintEventHandler {
        fn handle(&mut self, event: Event) {
            println!("{:?}",event)
        }
    }
    #[test]
    fn test_auth() {
        let addr = format!("{}:{}", "127.0.0.1", 6379);
        let mut stream = TcpStream::connect(addr).expect("connect err");
        stream
            .auth(Some("123".to_owned()), Some("123".to_owned()))
            .expect("auth fail");
    }

    #[test]
    fn test_ping() {
        let addr = format!("{}:{}", "127.0.0.1", 6379);
        let mut stream = TcpStream::connect(addr).expect("connect err");
        let _res = stream.ping().expect("ping fail");
    }

    #[test]
    fn test_replconf() {
        let addr = format!("{}:{}", "127.0.0.1", 6379);
        let mut stream = TcpStream::connect(addr).expect("connect err");
        stream
            .replconf("127.0.0.1".to_string(), 6380)
            .expect("ping fail");
    }

    #[test]
    fn test_eof() {
        assert_eq!("eof".to_redis_args()[0], b"eof")
    }

    #[test]
    fn test_psync() {
        let addr = format!("{}:{}", "127.0.0.1", 6379);
        let mut stream = TcpStream::connect(addr).expect("connect err");
        let socket_addr = stream.local_addr().unwrap();
        let local_ip = socket_addr.ip().to_string();
        let local_port = socket_addr.port();
        stream
            .replconf(local_ip, local_port)
            .expect("replconf fail");
        let res = stream
            .psync("?".to_string(), "-1".to_string())
            .expect("psync fail");
        println!("{:?}", res);
    }

    #[test]
    fn test_sync(){
        let addr = format!("{}:{}", "127.0.0.1", 6379);
        let mut stream = TcpStream::connect(addr).expect("connect err");
        let socket_addr = stream.local_addr().unwrap();
        let local_ip = socket_addr.ip().to_string();
        let local_port = socket_addr.port();
        stream
            .replconf(local_ip, local_port)
            .expect("replconf fail");
        let res = stream
            .psync("?".to_string(), "-1".to_string())
            .expect("psync fail");
        loop {
            match res.next_step {
                super::NextStep::FullSync => {
                    if res.length != -1 {
                        println!("Full Sync, size: {}bytes", res.length);
                    } else {
                        println!("Disk-less replication.");
                    }
                    let mut handler = PrintEventHandler{};
                    let _ = stream.parse(&mut handler, Arc::new(AtomicBool::new(true))).expect("pars rdb err");
                    break;
                },
                super::NextStep::PartialResync => todo!(),
                super::NextStep::ChangeMode => todo!(),
                super::NextStep::Wait => todo!(),
            };
        }
        if res.length==-1{
            println!("skip rd io");
            crate::skip(&mut stream,40).expect("skip err");
        }
        stream.replconf_ack(res.repl_offset.to_string()).expect("ack err");  
    }
}
