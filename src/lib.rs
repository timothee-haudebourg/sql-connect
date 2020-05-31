#![feature(vec_into_raw_parts)]

#[macro_use]
extern crate pin_utils;

use futures::{
	Stream,
	FutureExt,
	future::{
		Future,
		LocalBoxFuture
	}
};

mod error;
mod backoff;
mod backend;
mod value;
mod row;
mod parsing;
mod transaction;

pub use error::*;
pub use self::backoff::*;
pub use backend::*;
pub use value::*;
pub use row::*;
pub use transaction::*;

pub trait Connection: Sized {
	type Statement: Statement<Self>;

	/// Compile an SQL statement.
	///
	/// The string must consist of a single SQL statement,
	/// with no terminating semicolon (`;`).
	fn prepare(&mut self, sql: &str) -> Result<Option<Self::Statement>>;

	/// Compile a list of SQL statements.
	///
	/// Statements must be separated by a (`;`) semicolon.
	fn prepare_list(&mut self, sql: &str) -> Result<Vec<Self::Statement>> {
		let mut statements = Vec::new();

		for stmt in crate::parsing::split_statement_list(sql) {
			if let Some(prepared_stmt) = self.prepare(stmt)? {
				statements.push(prepared_stmt)
			}
		}

		Ok(statements)
	}

	/// Execute the given statement through this connection.
	///
	/// The statement must have been prepared by this connection.
	///
	/// Every pending statements will be executed before the given statement using the
	/// [`execute_pending_statements`] function.
	fn execute<'a, S: Statement<Self>, R: 'a + FromRow>(&mut self, statement: &'a S, args: Vec<Value>) -> LocalBoxFuture<'a, Result<Option<Rows<'a, R>>>> {
		statement.execute(self, args)
	}
}

pub trait Statement<C: Connection> {
	/// Execute the statement.
	/// If the statement is a data query, returns some stream of rows.
	fn execute<'a, R: 'a + FromRow>(&'a self, connection: &mut C, args: Vec<Value>) -> LocalBoxFuture<Result<Option<Rows<'a, R>>>>;
}
