use std::path::Path;
use std::marker::PhantomData;
use std::ffi::CString;
use std::os::raw::c_int;
use std::rc::Rc;
use std::pin::Pin;
use futures::{
	Stream,
	future::{
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

struct Inner {
	handle: *mut ffi::sqlite3
}

pub struct Connection {
	inner: Rc<Inner>
}

fn check(code: c_int) -> Result<()> {
	match code {
		ffi::SQLITE_OK => Ok(()),
		_ => panic!("TODO error")
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
				inner: Rc::new(Inner {
					handle: handle
				})
			})
		}
	}

	/// Compile an SQL statement.
	pub fn prepare(&self, sql: &str) -> Result<Statement> {
		unsafe {
			let mut handle = std::ptr::null_mut();
			check(ffi::sqlite3_prepare_v2(
				self.inner.handle,
				&sql.as_bytes()[0] as *const u8 as *const i8,
				sql.len() as i32,
				&mut handle,
				std::ptr::null_mut()
			))?;

			Ok(Statement {
				db: self.inner.clone(),
				handle
			})
		}
	}
}

pub struct Statement {
	db: Rc<Inner>,
	handle: *mut ffi::sqlite3_stmt
}

impl Statement {
	/// Try to execute the statement.
	///
	/// This is a non-blocking method. A `ErrorKind::Busy` error will be raised if the database
	/// is busy.
	pub fn try_execute<R>(&self) -> Result<Option<Rows<R>>> {
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

	pub async fn execute<'a, R>(&'a self) -> Result<Option<Rows<'a, R>>> {
		let mut backoff = backoff::ExponentialBackoff::default();
		async move { self.try_execute() }.with_backoff(&mut backoff).await
	}
}

impl crate::Statement for Statement {
	fn execute<'a, R: 'a + FromRow>(&'a self) -> LocalBoxFuture<Result<Option<Box<dyn 'a + Stream<Item = Result<R>>>>>> {
		async move {
			match self.execute().await {
				Ok(Some(rows)) => Ok(Some(Box::new(rows) as Box<dyn Stream<Item = Result<R>>>)),
				Ok(None) => Ok(None),
				Err(e) => Err(e)
			}
		}.boxed_local()
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
	row: PhantomData<R>
}

impl<'a, R> Rows<'a, R> {
	pub fn empty(statement: &'a Statement, column_count: usize) -> Rows<R> {
		Rows {
			statement,
			column_count,
			backoff: BackoffState::new(backoff::ExponentialBackoff::default()),
			row: PhantomData
		}
	}

	pub fn new(statement: &'a Statement, column_count: usize) -> Rows<R> {
		Rows {
			statement,
			column_count,
			backoff: BackoffState::new(backoff::ExponentialBackoff::default()),
			row: PhantomData
		}
	}

	unsafe_pinned!(backoff: BackoffState<backoff::ExponentialBackoff>);
}

impl<'a, R: FromRow> Stream for Rows<'a, R> {
	type Item = Result<R>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		unsafe {
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

impl<'a, R> Drop for Rows<'a, R> {
	fn drop(&mut self) {
		unsafe {
			ffi::sqlite3_reset(self.statement.handle)
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

impl<'a, R: FromRow> Iterator for Row<'a, R> {
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
						Value::Text(std::str::from_utf8_unchecked(bytes))
					},
					ffi::SQLITE_BLOB => {
						let len = ffi::sqlite3_column_bytes(self.rows.statement.handle, i) as usize;
						let ptr = ffi::sqlite3_column_blob(self.rows.statement.handle, i) as *const u8;
						Value::Blob(std::slice::from_raw_parts(ptr, len))
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
