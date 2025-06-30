use tokio::io::{
    self, AsyncBufReadExt, AsyncRead, AsyncSeek, AsyncSeekExt, BufReader,
};

#[derive(Debug)]
pub enum Statement {
    Comment(String),
    Query(String),
}

/// Streams `Statement`s out of any `AsyncRead + AsyncSeek` source.
///
/// * Lines that begin with `--` are comments (newline **excluded**).
/// * Everything else is part of a query until the first `;\n` terminator
///   (terminator **included**).
///
/// Each `next_statement()` returns the byte offset **from the start of the
/// file/stream** where that statement begins.
pub struct StatementStream<R>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    reader: BufReader<R>,
    pending: String,
    buf: Vec<u8>,
    stmt_start_pos: Option<u64>,
}

impl<R> StatementStream<R>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    /// 16 KiB is small-cache-friendly but usually completed in one read(2).
    pub fn new(inner: R) -> Self {
        Self {
            reader: BufReader::with_capacity(16 * 1024, inner),
            pending: String::new(),
            buf: Vec::with_capacity(1024),
            stmt_start_pos: None,
        }
    }

    /// Retrieve the underlying reader when you’re done.
    pub fn into_inner(self) -> R {
        self.reader.into_inner()
    }

    /// Async counterpart of the synchronous version.
    ///
    /// Returns `None` at clean EOF.
    pub async fn next_statement(
        &mut self,
    ) -> Option<Result<(u64, Statement), io::Error>> {
        loop {
            self.buf.clear();

            // Position before we read this line
            let pos_before_line = match self.reader.stream_position().await {
                Ok(p) => p,
                Err(e) => return Some(Err(e)),
            };

            // Read a line (including `\n`)
            let n = match self.reader.read_until(b'\n', &mut self.buf).await {
                Ok(0) if self.pending.is_empty() => return None, // clean EOF
                Ok(0) => {
                    // EOF mid-query ⇒ return what we have
                    let q = std::mem::take(&mut self.pending);
                    let start = self.stmt_start_pos.take().unwrap_or(0);
                    return Some(Ok((start, Statement::Query(q))));
                }
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };

            // Validate UTF-8
            let line = match std::str::from_utf8(&self.buf[..n]) {
                Ok(s) => s,
                Err(_) => {
                    return Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "input is not valid UTF-8",
                    )))
                }
            };

            // Comment fast-path
            if line.starts_with("--") {
                debug_assert!(
                    self.pending.is_empty(),
                    "comments inside statements are forbidden by spec"
                );
                let comment = line.trim_end_matches('\n').to_owned();
                return Some(Ok((pos_before_line, Statement::Comment(comment))));
            }

            // Otherwise we’re building (or starting) a query
            if self.pending.is_empty() {
                self.stmt_start_pos = Some(pos_before_line);
            }
            self.pending.push_str(line);

            // Query complete?
            if self.pending.ends_with(";\n") {
                let q = std::mem::take(&mut self.pending);
                let start = self.stmt_start_pos.take().unwrap_or(pos_before_line);
                return Some(Ok((start, Statement::Query(q))));
            }
        }
    }
}
