use std::io;
use std::io::{Cursor, Error, ErrorKind, Read};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::rdb::{read_zip_list_entry, read_zm_len, Field, Item, RDBDecode};

pub(crate) trait Iter {
    fn next<T: Read>(&mut self, input: T) -> io::Result<Vec<u8>>;
}

pub(crate) struct StrValIter {
    pub(crate) count: isize,
}

impl Iter for StrValIter {
    fn next<T: Read>(&mut self, mut input: T) -> io::Result<Vec<u8>> {
        if self.count > 0 {
            let val = input.read_string()?;
            self.count -= 1;
            return Ok(val);
        };
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}

pub(crate) struct QuickListIter {
    pub(crate) len: isize,
    pub(crate) count: isize,

    pub(crate) cursor: Option<Cursor<Vec<u8>>>,
}

impl Iter for QuickListIter {
    fn next<T: Read>(&mut self, mut input: T) -> io::Result<Vec<u8>> {
        if self.len == -1 && self.count > 0 {
            let data = input.read_string()?;
            self.cursor = Option::Some(Cursor::new(data));

            let cursor = self.cursor.as_mut().unwrap();
            cursor.set_position(8);
            self.len = cursor.read_i16::<LittleEndian>()? as isize;
            if self.len == 0 {
                self.len = -1;
                self.count -= 1;
            }
            if self.has_more() {
                return self.next(input);
            }
        } else if self.count > 0 {
            let val = read_zip_list_entry(self.cursor.as_mut().unwrap())?;
            self.len -= 1;
            if self.len == 0 {
                self.len = -1;
                self.count -= 1;
            }
            return Ok(val);
        }
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}

impl QuickListIter {
    fn has_more(&self) -> bool {
        self.len > 0 || self.count > 0
    }
}

// ZipList的值迭代器
pub(crate) struct ZipListIter<'a> {
    pub(crate) count: isize,
    pub(crate) cursor: &'a mut Cursor<Vec<u8>>,
}

impl Iter for ZipListIter<'_> {
    fn next<T: Read>(&mut self, _input: T) -> io::Result<Vec<u8>> {
        if self.count > 0 {
            let val = read_zip_list_entry(self.cursor)?;
            self.count -= 1;
            return Ok(val);
        }
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}

// SortedSet的值迭代器
pub(crate) struct SortedSetIter {
    pub(crate) count: isize,
    /// v = 1, zset
    /// v = 2, zset2
    pub(crate) v: u8,
    //pub(crate) input: &'a mut dyn Read,
}

impl SortedSetIter {
    pub fn next<T: Read>(&mut self, mut input: T) -> io::Result<Item> {
        if self.count > 0 {
            let member = input.read_string()?;
            let score = if self.v == 1 {
                 input.read_double()?
            } else {
                let score_u64 = input.read_u64::<LittleEndian>()?;
                 f64::from_bits(score_u64)
            };
            self.count -= 1;
            return Ok(Item { member, score });
        }
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}

// HashZipMap的值迭代器
pub(crate) struct ZipMapIter<'a> {
    pub(crate) has_more: bool,
    pub(crate) cursor: &'a mut Cursor<&'a Vec<u8>>,
}

impl ZipMapIter<'_> {
    pub(crate) fn next(&mut self) -> io::Result<Field> {
        if !self.has_more {
            return Err(Error::new(ErrorKind::NotFound, "No element left"));
        }
        let zm_len = read_zm_len(self.cursor)?;
        if zm_len == 255 {
            self.has_more = false;
            return Err(Error::new(ErrorKind::NotFound, "No element left"));
        }
        let mut field = vec![0; zm_len];
        self.cursor.read_exact(&mut field)?;
        let zm_len = read_zm_len(self.cursor)?;
        if zm_len == 255 {
            self.has_more = false;
            return Ok(Field {
                name: field,
                value: Vec::new(),
            });
        };
        let free = self.cursor.read_i8()?;
        let mut val = vec![0; zm_len];
        self.cursor.read_exact(&mut val)?;
        self.cursor
            .set_position(self.cursor.position() + free as u64);
        Ok(Field {
            name: field,
            value: val,
        })
    }
}

// IntSet的值迭代器
pub(crate) struct IntSetIter<'a> {
    pub(crate) encoding: i32,
    pub(crate) count: isize,
    pub(crate) cursor: &'a mut Cursor<&'a Vec<u8>>,
}

impl Iter for IntSetIter<'_> {
    fn next<T: Read>(&mut self, _input: T) -> io::Result<Vec<u8>> {
        if self.count > 0 {
            let val = match self.encoding {
                2 => {
                    let member = self.cursor.read_i16::<LittleEndian>()?;
                    
                    member.to_string().into_bytes()
                }
                4 => {
                    let member = self.cursor.read_i32::<LittleEndian>()?;
                    
                    member.to_string().into_bytes()
                }
                8 => {
                    let member = self.cursor.read_i64::<LittleEndian>()?;
                    
                    member.to_string().into_bytes()
                }
                _ => panic!("Invalid integer size: {}", self.encoding),
            };
            self.count -= 1;
            return Ok(val);
        }
        Err(Error::new(ErrorKind::NotFound, "No element left"))
    }
}
