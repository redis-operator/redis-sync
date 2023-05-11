use anyhow::{anyhow, Result};

use crate::resp::{Resp, RespDecode, Type};
use redis::ToRedisArgs;
use std::io::{Read, Write};

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
                println!("{:?}", s);
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
        args.extend("PING".to_redis_args());
        let result = redis::pack_command(&args);
        self.write_all(&result)?;
        self.reply()?;

        let mut args = vec![];
        args.extend("REPLCONF".to_redis_args());
        args.extend("listening-port".to_redis_args());
        args.extend(port.to_redis_args());
        let result = redis::pack_command(&args);
        self.write_all(&result)?;
        self.reply()?;

        if ip!="127.0.0.1"{
        let mut args = vec![];
        args.extend("REPLCONF".to_redis_args());
        args.extend("ip-address".to_redis_args());
        args.extend(ip.to_redis_args());
        let result = redis::pack_command(&args);
        self.write_all(&result)?;
        self.reply()?;
        }
        let mut args = vec![];
        args.extend("REPLCONF".to_redis_args());
        args.extend("capa".to_redis_args());
        args.extend("eof".to_redis_args());
        args.extend("capa".to_redis_args());
        args.extend("psync2".to_redis_args());

        let result = redis::pack_command(&args);
        self.write_all(&result)?;
        self.reply()

        // let mut args = vec![];
        // args.extend("REPLCONF".to_redis_args());
        // args.extend("capa".to_redis_args());
        // args.extend("psync2".to_redis_args());

        // let result = redis::pack_command(&args);
        // self.write_all(&result)?;
        // self.reply()?;

        // let mut args = vec![];
        // args.extend("REPLCONF".to_redis_args());
        // args.extend("RDBONLY".to_redis_args());
        // args.extend("0".to_redis_args());

        // let result = redis::pack_command(&args);
        // self.write_all(&result)?;
        // self.reply()

    }

    fn psync(&mut self, repl_id: String, repl_offset: String) -> Result<PsyncResp> {
        let mut args = vec![];

        args.extend("PSYNC".to_redis_args());
        args.extend(repl_id.to_redis_args());
        args.extend(repl_offset.to_redis_args());
        let result = redis::pack_command(&args);
        self.write_all(&result)?;

        match self.decode_resp() {
            Err(err) => {
                return Err(anyhow!(" decode_resp err:{:?} ", err));
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
                    } else if resp.starts_with("NOMASTERLINK") || resp.starts_with("LOADING") {
                        next_type = NextStep::Wait;
                        length = -1;
                    };
                    return Ok(PsyncResp {
                        next_step: next_type,
                        repl_id,
                        repl_offset,
                        length,
                    });
                }
            }
        }
        Ok(PsyncResp {
            next_step: NextStep::Wait,
            repl_id: "".to_string(),
            repl_offset: 0,
            length: 0,
        })
    }

    fn replconf_ack(&mut self, repl_offset: String) -> Result<()> {
        
        // let mut args = vec![];
        // args.extend("ping".to_redis_args());
        // let result = redis::pack_command(&args);
        // self.write_all(&result)?;

        let mut args = vec![];
        args.extend("REPLCONF".to_redis_args());
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
pub struct PsyncResp {
    pub next_step: NextStep,
    pub repl_id: String,
    pub repl_offset: i64,
    pub length: i64,
}

#[cfg(test)]
mod test {
    use crate::{
        cmd,
        rdb::{RDBDecode, RDBParser},
        resp::{Resp, RespDecode},
    };

    use super::Connect;
    use crate::{Event, EventHandler};
    use byteorder::ReadBytesExt;
    use redis::ToRedisArgs;
    use std::{
        io::{self, Read, Write},
        net::TcpStream,
        sync::{atomic::{AtomicBool, AtomicI64, Ordering}, Arc},
        thread::{sleep, self},
        time::Duration, ops::Add, process::Command,
    };

    pub struct PrintlnEventHandler {}

    impl EventHandler for PrintlnEventHandler {
        fn handle(&mut self, event: Event) {
            println!("{:?}", event)
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
    fn test_sync() {
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
                    let mut handler = PrintlnEventHandler {};
                    let _ = stream
                        .parse(&mut handler, Arc::new(AtomicBool::new(true)))
                        .expect("pars rdb err");
                    if res.length == -1 {
                        println!("skip rdb io");
                        crate::io::skip(&mut stream, 40).expect("skip err");
                    }
                    break;
                }
                super::NextStep::PartialResync => todo!(),
                super::NextStep::ChangeMode => todo!(),
                super::NextStep::Wait => {sleep(Duration::from_secs(1))},
            };
        }
        
        let repl_offset = Arc::new(AtomicI64::from(res.repl_offset));
        let repl_offset_arc = Arc::clone(&repl_offset);
        let mut conn_clone = stream.try_clone().unwrap();
        let handle = thread::Builder::new()
        .name("redis-sync background".to_string())
        .spawn(move || {
            loop{
            let offset_str = repl_offset_arc.load(Ordering::Relaxed).to_string();
            println!("ack {}",offset_str);
            conn_clone.replconf_ack(offset_str).expect("replconf_err");
            sleep(Duration::from_secs(3))
            }
        })
        .unwrap();
        

        let mut handler = PrintlnEventHandler {};
        

        let mut stream_with_counter = crate::io::CountReader::new(&mut stream);
        
        println!("start aof loop");
        sleep(Duration::from_secs(2));
        loop {
            stream_with_counter.mark();
            if let Resp::Array(array) = stream_with_counter.decode_resp().expect("err") {
                let size = stream_with_counter.reset().expect("reset err");
                let mut vec = Vec::with_capacity(array.len());
                for x in array {
                    if let Resp::BulkBytes(bytes) = x {
                        vec.push(bytes);
                    } else {
                        panic!("Expected BulkString response");
                    }
                }
                cmd::parse(vec, &mut handler);
                let offset = repl_offset.load(Ordering::Relaxed);
                repl_offset.store(offset+size, Ordering::SeqCst);
                // clone_stream.replconf_ack(offset.to_string()).expect("err");
            } else {
                panic!("Expected array response");
            }
        }
    }

    fn start_redis_server(rdb: &str, port: u16) -> u32 {
        // redis-server --port 6379 --daemonize no --dbfilename rdb --dir ./tests/rdb
        let child = Command::new("redis-server")
            .arg("--port")
            .arg(port.to_string())
            .arg("--daemonize")
            .arg("no")
            .arg("--dbfilename")
            .arg(rdb)
            .arg("--dir")
            .arg("./tests/rdb")
            .arg("--loglevel")
            .arg("warning")
            .arg("--logfile")
            .arg(port.to_string())
            .spawn()
            .expect("failed to start redis-server");
        return child.id();
    }

    fn start_auth_redis_server(rdb: &str, port: u16) -> u32 {
        // redis-server --port 6379 --daemonize no --dbfilename rdb --dir ./tests/rdb
        let child = Command::new("redis-server")
            .arg("--port")
            .arg(port.to_string())
            .arg("--daemonize")
            .arg("no")
            .arg("--dbfilename")
            .arg(rdb)
            .arg("--dir")
            .arg("./tests/rdb")
            .arg("--loglevel")
            .arg("warning")
            .arg("--logfile")
            .arg(port.to_string())
            .arg("--requirepass")
            .arg("123")
            .spawn()
            .expect("failed to start redis-server");
        return child.id();
    }

    fn shutdown_redis(pid: u32) {
        Command::new("kill")
            .arg("-9")
            .arg(pid.to_string())
            .status()
            .expect("kill redis failed");
    }
}
