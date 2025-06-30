use std::io::{self, BufRead, BufReader, Read};

#[derive(Debug)]
pub enum Statement {
    Comment(String),
    Query(String),
}

pub struct StatementStream<R: Read> {
    reader: BufReader<R>,
    /// Accumulates the current query until we see a terminating “;\n”.
    pending: String,
    /// Scratch buffer reused for each `read_until`.
    buf: Vec<u8>,
}

impl<R: Read> StatementStream<R> {
    /// 16 KiB is large enough that the kernel almost always completes each
    /// `read(2)` in a single syscall but small enough to stay CPU-cache-friendly.
    pub fn new(inner: R) -> Self {
        Self {
            reader: BufReader::with_capacity(16 * 1024, inner),
            pending: String::new(),
            buf: Vec::with_capacity(1024),
        }
    }

    /// Retrieve the underlying reader once you’re done parsing.
    pub fn into_inner(self) -> R {
        self.reader.into_inner()
    }

    /// Returns the next `Statement`, or `None` at EOF.
    ///
    /// * A comment starts with “--” and extends to the newline (the newline is **not**
    ///   included in the returned string, matching typical SQL comment semantics).
    /// * A query is everything from its first non-comment line up to and **including**
    ///   the first “;\n”.
    pub fn next_statement(&mut self) -> Option<Result<Statement, io::Error>> {
        loop {
            self.buf.clear();

            // Read one (possibly very long) line, including the '\n'.
            let n = match self.reader.read_until(b'\n', &mut self.buf) {
                Ok(0) if self.pending.is_empty() => return None,                    // clean EOF
                Ok(0) /* EOF in the middle of a query */ => {
                    let q = std::mem::take(&mut self.pending);
                    return Some(Ok(Statement::Query(q)));
                }
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };

            // Convert the read bytes to UTF-8; if it’s not valid, propagate an error.
            let line = match std::str::from_utf8(&self.buf[..n]) {
                Ok(s) => s,
                Err(_) => {
                    return Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "input is not valid UTF-8",
                    )))
                }
            };

            // Fast path: comment line.
            if line.starts_with("--") {
                debug_assert!(self.pending.is_empty(),
                              "input mixes comment lines inside statements, which the spec forbids");
                // Strip the trailing newline if present.
                let comment = line.trim_end_matches('\n').to_owned();
                return Some(Ok(Statement::Comment(comment)));
            }

            // Otherwise we’re in (or starting) a query.
            self.pending.push_str(line);

            // We’ve finished a query when the accumulator ends with “;\n”.
            if self.pending.ends_with(";\n") {
                let q = std::mem::take(&mut self.pending);
                return Some(Ok(Statement::Query(q)));
            }
        }
    }
}
