use std::path::Path;
use std::marker::PhantomData;
use std::fmt;
use std::ffi::CString;
use std::os::raw::{
	c_void,
	c_char,
	c_int
};
use std::pin::Pin;
use mown::Mown;
use futures::{
	Stream,
	future::{
		Future,
		LocalBoxFuture,
		FutureExt
	}
};
use std::task::{
	Poll,
	Context
};
use libsqlite3_sys as ffi;

use crate::{
	Result,
	ErrorKind,
	FromRow,
	Value,
	backoff::{
		BackoffExt,
		BackoffState
	}
};

pub struct Connection {
	handle: *mut ffi::sqlite3,
	next_savepoint: usize
}

unsafe impl Send for Connection { }

#[derive(Debug)]
pub enum SqliteError {
	Unknown,
	Internal,
	Perm,
	Abort,
	Busy,
	Locked,
	NoMem,
	ReadOnly,
	Interrupt,
	IO,
	Corrupt,
	NotFound,
	Full,
	CantOpen,
	Protocol,
	Empty,
	Schema,
	TooBig,
	Constraint,
	Mismatch,
	Misuse,
	NoLFS,
	Auth,
	Format,
	Range,
	NotADB
}

impl fmt::Display for SqliteError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{:?}", self)
	}
}

impl std::error::Error for SqliteError {
	//
}

impl From<SqliteError> for crate::Error {
	fn from(e: SqliteError) -> crate::Error {
		let kind = match e {
			SqliteError::Schema => ErrorKind::SchemaChanged,
			_ => ErrorKind::Failure
		};

		crate::Error::new(kind, Some(Box::new(e)))
	}
}

fn check(code: c_int) -> std::result::Result<(), SqliteError> {
	let primary = code & 0xff;
	let extended = code >> 8;

	match primary {
		ffi::SQLITE_OK => Ok(()),
		ffi::SQLITE_ERROR => {
			println!("code: {}", code);
			Err(SqliteError::Unknown)
		},
		ffi::SQLITE_INTERNAL => Err(SqliteError::Internal),
		ffi::SQLITE_PERM => Err(SqliteError::Perm),
		ffi::SQLITE_ABORT => Err(SqliteError::Abort),
		ffi::SQLITE_BUSY => Err(SqliteError::Busy),
		ffi::SQLITE_LOCKED => Err(SqliteError::Locked),
		ffi::SQLITE_NOMEM => Err(SqliteError::NoMem),
		ffi::SQLITE_READONLY => Err(SqliteError::ReadOnly),
		ffi::SQLITE_INTERRUPT => Err(SqliteError::Interrupt),
		ffi::SQLITE_IOERR => Err(SqliteError::IO),
		ffi::SQLITE_CORRUPT => Err(SqliteError::Corrupt),
		ffi::SQLITE_NOTFOUND => Err(SqliteError::NotFound),
		ffi::SQLITE_FULL => Err(SqliteError::Full),
		ffi::SQLITE_CANTOPEN => Err(SqliteError::CantOpen),
		ffi::SQLITE_PROTOCOL => Err(SqliteError::Protocol),
		ffi::SQLITE_EMPTY => Err(SqliteError::Empty),
		ffi::SQLITE_SCHEMA => Err(SqliteError::Schema),
		ffi::SQLITE_TOOBIG => Err(SqliteError::TooBig),
		ffi::SQLITE_CONSTRAINT => Err(SqliteError::Constraint),
		ffi::SQLITE_MISMATCH => Err(SqliteError::Mismatch),
		ffi::SQLITE_MISUSE => Err(SqliteError::Misuse),
		ffi::SQLITE_NOLFS => Err(SqliteError::NoLFS),
		ffi::SQLITE_AUTH => Err(SqliteError::Auth),
		ffi::SQLITE_FORMAT => Err(SqliteError::Format),
		ffi::SQLITE_RANGE => Err(SqliteError::Range),
		ffi::SQLITE_NOTADB => Err(SqliteError::NotADB),
		_ => Err(SqliteError::Unknown)
	}
}

// fn str_to_cstring(s: &str) -> Result<CString> {
// 	Ok(CString::new(s).map_err(|_| ErrorKind::InvalidString(s.to_string()).err())?)
// }

#[cfg(unix)]
fn path_to_cstring(p: &Path) -> Result<CString> {
	use std::os::unix::ffi::OsStrExt;
	Ok(CString::new(p.as_os_str().as_bytes()).map_err(|_| ErrorKind::InvalidPath(p.to_owned()).err())?)
}

#[cfg(not(unix))]
fn path_to_cstring(p: &Path) -> Result<CString> {
	let s = p.to_str().ok_or_else(|| ErrorKind::InvalidPath(p.to_owned()).err())?;
	Ok(CString::new(s)?)
}

impl Connection {
	/// Creates an in-memory connection.
	///
	/// This is equivalent to `Connection::open(":memory:")`.
	pub fn new() -> Result<Connection> {
		Self::open(":memory:")
	}

	/// Open a new connection to the given file path.
	///
	/// If path is `:memory:`, it will open a new in-memory connection.
	pub fn open<P: AsRef<Path>>(path: P) -> Result<Connection> {
		unsafe {
			let mut handle = std::ptr::null_mut();
			let c_path = path_to_cstring(path.as_ref())?;
			check(ffi::sqlite3_open(c_path.as_ptr(), &mut handle))?;
			Ok(Connection {
				handle: handle,
				next_savepoint: 0
			})
		}
	}
}

impl crate::Connection for Connection {
	type Statement = Statement;

	/// Compile an SQL statement.
	///
	/// The string must consist of a single SQL statement,
	/// with no terminating semicolon (`;`).
	fn prepare(&mut self, sql: &str) -> Result<Option<Statement>> {
		unsafe {
			let mut handle = std::ptr::null_mut();
			check(ffi::sqlite3_prepare_v2(
				self.handle,
				&sql.as_bytes()[0] as *const u8 as *const i8,
				sql.len() as i32,
				&mut handle,
				std::ptr::null_mut()
			))?;

			if handle.is_null() {
				Ok(None)
			} else {
				Ok(Some(Statement {
					handle
				}))
			}
		}
	}

	fn execute<'a, R: 'a + FromRow>(&mut self, statement: &'a Self::Statement, args: Vec<Value>) -> LocalBoxFuture<'a, Result<Option<crate::Rows<'a, R>>>> {
		let exec = statement.execute(self, args);
		async move {
			match exec.await {
				Ok(Some(rows)) => {
					Ok(Some(crate::Rows::new(rows)))
				},
				Ok(None) => Ok(None),
				Err(e) => Err(e)
			}
		}.boxed_local()
	}
}

impl Drop for Connection {
	fn drop(&mut self) {
		unsafe {
			ffi::sqlite3_close(self.handle);
		}
	}
}

impl crate::TransactionCapable for Connection { }

impl crate::SavepointCapable for Connection {
	fn anonymous_savepoint_name(&mut self) -> String {
		let i = self.next_savepoint;
		self.next_savepoint += 1;
		"anon".to_string() + &i.to_string()
	}
}

pub struct Statement {
	handle: *mut ffi::sqlite3_stmt
}

impl Statement {
	fn bind(&self, index: usize, value: Value) -> Result<()> {
		unsafe {
			let i = index as i32 + 1;
			let res = match value {
				Value::Integer(n) => ffi::sqlite3_bind_int64(self.handle, i, n),
				Value::Float(f) => ffi::sqlite3_bind_double(self.handle, i, f),
				Value::Text(str) => ffi::sqlite3_bind_text(self.handle, i, str.as_ptr() as *const c_char, str.len() as i32, ffi::SQLITE_TRANSIENT()),
				Value::Blob(blob) => ffi::sqlite3_bind_blob(self.handle, i, blob.as_ptr() as *const c_void, blob.len() as i32, ffi::SQLITE_TRANSIENT()),
				Value::Null => ffi::sqlite3_bind_null(self.handle, i)
			};

			check(res)?;
			Ok(())
		}
	}

	fn bind_all(&self, args: Vec<Value>) -> Result<()> {
		let mut i = 0;
		for arg in args {
			self.bind(i, arg)?;
			i += 1;
		}

		Ok(())
	}

	/// Try to execute the statement.
	///
	/// This is a non-blocking method. A `ErrorKind::Busy` error will be raised if the database
	/// is busy.
	fn try_execute<R>(&self, args: Vec<Value>) -> Result<Option<Rows<R>>> {
		self.bind_all(args);
		unsafe {
			let column_count = ffi::sqlite3_column_count(self.handle);
			match ffi::sqlite3_step(self.handle) {
				ffi::SQLITE_DONE => {
					if column_count > 0 {
						Ok(Some(Rows::empty(self, column_count as usize)))
					} else {
						Ok(None)
					}
				},
				ffi::SQLITE_ROW => {
					Ok(Some(Rows::new(self, column_count as usize)))
				},
				res => {
					check(res)?;
					Ok(None)
				}
			}
		}
	}

	fn execute<'a, R>(&'a self, _connection: &mut Connection, args: Vec<Value>) -> impl 'a + Future<Output=Result<Option<Rows<'a, R>>>> {
		let mut backoff = backoff::ExponentialBackoff::default();
		self.bind_all(args);
		async move {
			async move { self.try_execute(Vec::new()) }.with_backoff(&mut backoff).await
		}
	}
}

impl Drop for Statement {
	fn drop(&mut self) {
		unsafe {
			ffi::sqlite3_finalize(self.handle);
		}
	}
}

pub struct Rows<'a, R> {
	statement: &'a Statement,
	column_count: usize,
	backoff: BackoffState<backoff::ExponentialBackoff>,
	first_row: bool,
	row: PhantomData<R>
}

impl<'a, R> Rows<'a, R> {
	pub fn empty(statement: &'a Statement, column_count: usize) -> Rows<R> {
		Rows {
			statement,
			column_count,
			backoff: BackoffState::new(backoff::ExponentialBackoff::default()),
			first_row: false,
			row: PhantomData
		}
	}

	pub fn new(statement: &'a Statement, column_count: usize) -> Rows<R> {
		Rows {
			statement,
			column_count,
			backoff: BackoffState::new(backoff::ExponentialBackoff::default()),
			first_row: true,
			row: PhantomData
		}
	}

	pub fn consume(&mut self) {
		self.first_row = false;
	}

	unsafe_pinned!(backoff: BackoffState<backoff::ExponentialBackoff>);
}

impl<'a, R> Unpin for Rows<'a, R> { }

impl<'a, R: FromRow> Stream for Rows<'a, R> {
	type Item = Result<R>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		unsafe {
			if self.first_row {
				self.first_row = false;
				let row = Row::new(&self);
				Poll::Ready(Some(Ok(R::from(row))))
			} else {
				match ffi::sqlite3_step(self.statement.handle) {
					ffi::SQLITE_DONE => {
						Poll::Ready(None)
					},
					ffi::SQLITE_ROW => {
						let row = Row::new(&self);
						Poll::Ready(Some(Ok(R::from(row))))
					},
					ffi::SQLITE_BUSY => {
						match self.backoff().poll(cx) {
							Ok(()) => Poll::Pending,
							Err(e) => Poll::Ready(Some(Err(e)))
						}
					},
					res => {
						check(res)?;
						unreachable!()
					}
				}
			}
		}
	}
}

impl<'a, R> Drop for Rows<'a, R> {
	fn drop(&mut self) {
		unsafe {
			ffi::sqlite3_reset(self.statement.handle);
		}
	}
}

pub struct Row<'a, R> {
	rows: &'a Rows<'a, R>,
	index: usize
}

impl<'a, R> Row<'a, R> {
	fn new(rows: &'a Rows<'a, R>) -> Row<'a, R> {
		Row {
			rows,
			index: 0
		}
	}
}

impl<'a, R> Iterator for Row<'a, R> {
	type Item = Value<'a>;

	fn next(&mut self) -> Option<Value<'a>> {
		if self.index < self.rows.column_count {
			let i = self.index as i32;
			let column = unsafe {
				match ffi::sqlite3_column_type(self.rows.statement.handle, i) {
					ffi::SQLITE_INTEGER => Value::Integer(ffi::sqlite3_column_int64(self.rows.statement.handle, i)),
					ffi::SQLITE_FLOAT => Value::Float(ffi::sqlite3_column_double(self.rows.statement.handle, i)),
					ffi::SQLITE_TEXT => {
						let len = ffi::sqlite3_column_bytes(self.rows.statement.handle, i) as usize;
						let ptr = ffi::sqlite3_column_text(self.rows.statement.handle, i) as *const u8;
						let bytes = std::slice::from_raw_parts(ptr, len);
						Value::Text(Mown::Borrowed(std::str::from_utf8_unchecked(bytes)))
					},
					ffi::SQLITE_BLOB => {
						let len = ffi::sqlite3_column_bytes(self.rows.statement.handle, i) as usize;
						let ptr = ffi::sqlite3_column_blob(self.rows.statement.handle, i) as *const u8;
						Value::Blob(Mown::Borrowed(std::slice::from_raw_parts(ptr, len)))
					},
					_ => Value::Null
				}
			};

			self.index += 1;
			Some(column)
		} else {
			None
		}
	}
}
