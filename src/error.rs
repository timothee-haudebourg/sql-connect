use std::fmt;
use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error {
	kind: ErrorKind,
	source: Option<Box<dyn std::error::Error>>
}

impl Error {
	pub fn new(kind: ErrorKind, source: Option<Box<dyn std::error::Error>>) -> Error {
		Error {
			kind,
			source
		}
	}

	pub fn kind(&self) -> &ErrorKind {
		&self.kind
	}

	// pub fn backoff(self) -> backoff::Error<Error> {
	// 	if self.kind.is_busy() {
	// 		backoff::Error::Transient(self)
	// 	} else {
	// 		backoff::Error::Permanent(self)
	// 	}
	// }
}

#[derive(Clone, Debug)]
pub enum ErrorKind {
	InvalidString(String),
	InvalidPath(PathBuf),
	InvalidQuery,
	Failure,

	/// The database is busy.
	Busy,

	/// The database schema changed since the statement was prepared.
	SchemaChanged,
}

impl ErrorKind {
	pub fn err(self) -> Error {
		Error {
			kind: self,
			source: None
		}
	}

	pub fn is_busy(&self) -> bool {
		match self {
			ErrorKind::Busy => true,
			_ => false
		}
	}
}

impl fmt::Display for ErrorKind {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		use ErrorKind::*;
		match self {
			InvalidString(_) => write!(f, "invalid string"),
			InvalidPath(_) => write!(f, "invalid path"),
			InvalidQuery => write!(f, "invalid query"),
			Failure => write!(f, "failure"),
			Busy => write!(f, "busy"),
			SchemaChanged => write!(f, "schema changed")
		}
	}
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		self.kind.fmt(f)
	}
}
