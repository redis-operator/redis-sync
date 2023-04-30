/*!
HyperLogLog相关的命令定义、解析

所有涉及到的命令参考[Redis Command Reference]

[Redis Command Reference]: https://redis.io/commands#hyperloglog
*/

use std::slice::Iter;

#[derive(Debug)]
pub struct PFADD<'a> {
    pub key: &'a [u8],
    pub elements: Vec<&'a [u8]>,
}

pub(crate) fn parse_pfadd(mut iter: Iter<Vec<u8>>) -> PFADD {
    let key = iter.next().unwrap();
    let mut elements = Vec::new();
    for element in iter {
        elements.push(element.as_slice());
    }
    PFADD { key, elements }
}

#[derive(Debug)]
pub struct PFCOUNT<'a> {
    pub keys: Vec<&'a [u8]>,
}

pub(crate) fn parse_pfcount(iter: Iter<Vec<u8>>) -> PFCOUNT {
    let mut keys = Vec::new();
    for key in iter {
        keys.push(key.as_slice());
    }
    PFCOUNT { keys }
}

#[derive(Debug)]
pub struct PFMERGE<'a> {
    pub dest_key: &'a [u8],
    pub source_keys: Vec<&'a [u8]>,
}

pub(crate) fn parse_pfmerge(mut iter: Iter<Vec<u8>>) -> PFMERGE {
    let dest_key = iter.next().unwrap();
    let mut source_keys = Vec::new();
    for source in iter {
        source_keys.push(source.as_slice());
    }
    PFMERGE { dest_key, source_keys }
}
