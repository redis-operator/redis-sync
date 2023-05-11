
use std::io::{self,BufReader, Error, ErrorKind, Read, Result};


pub(crate) struct CountReader<'a> {
    input: BufReader<&'a mut dyn Read>,
    len: i64,
    marked: bool,
}


impl Read for CountReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let len = self.input.read(buf)?;
        if self.marked {
            self.len += len as i64;
        };
        Ok(len)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.input.read_exact(buf)?;
        if self.marked {
            self.len += buf.len() as i64;
        };
        Ok(())
    }
}

impl CountReader<'_> {
    pub(crate) fn new(input: &mut dyn Read) -> CountReader {
        CountReader {
            input: BufReader::new(input),
            len: 0,
            marked: false,
        }
    }

    pub(crate) fn mark(&mut self) {
        self.marked = true;
    }

    pub(crate) fn reset(&mut self) -> Result<i64> {
        if self.marked {
            let len = self.len;
            self.len = 0;
            self.marked = false;
            return Ok(len);
        }
        Err(Error::new(ErrorKind::Other, "not marked"))
    }
}


pub(crate) fn skip(input: &mut dyn Read, length: u64) -> Result<()> {
    // used for diskless transfers
    // when the master does not know beforehand the size of the file to
    // transfer. In the latter case, the following format is used:
    // $EOF:<40 bytes delimiter>
        io::copy(&mut input.take(length), &mut io::sink())?;
        Ok(())
    }

