#![feature(vec_into_raw_parts)]

#[macro_use]
extern crate pin_utils;

use futures::{
	FutureExt,
	future::{
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
	type Statement;

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
	fn execute<'a, R: 'a + FromRow>(&'a mut self, statement: &'a Self::Statement, args: Vec<Value>) -> LocalBoxFuture<'a, Result<Option<Rows<'a, R>>>>;

	/// Execute the statement by consuming it.
	fn consume<'a, R: 'a + FromRow>(&'a mut self, statement: Self::Statement, args: Vec<Value>) -> LocalBoxFuture<'a, Result<Option<OwnedRows<'a, Self::Statement, R>>>> where Self::Statement: 'a {
		unsafe {
			// This is safe because the statement will be embeded in the `OwnedRows` so that it won't be dropped before the rows.
			let exec: LocalBoxFuture<'a, Result<Option<Rows<'a, R>>>> = std::mem::transmute(self.execute::<R>(&statement, args));
			async move {
				match exec.await? {
					Some(rows) => Ok(Some(rows.into_owned(statement))),
					None => Ok(None)
				}
			}.boxed_local()
		}
	}

	/// Prepare and execute a statement.
	fn execute_sql<'a, R: 'a + FromRow>(&'a mut self, sql: &str, args: Vec<Value>) -> LocalBoxFuture<'a, Result<Option<OwnedRows<'a, Self::Statement, R>>>> where Self::Statement: 'a {
		match self.prepare(sql) {
			Ok(Some(statement)) => {
				self.consume(statement, args)
			},
			Ok(None) => async move {
				Ok(None)
			}.boxed_local(),
			Err(e) => async move {
				Err(e)
			}.boxed_local()
		}
	}

	/// Prepare and execute a statement.
	fn execute_script<'a>(&'a mut self, sql: &'a str) -> LocalBoxFuture<'a, Result<()>> where Self::Statement: 'a {
		async move {
			for stmt in crate::parsing::split_statement_list(sql) {
				if let Some(prepared_stmt) = self.prepare(stmt)? {
					self.execute::<()>(&prepared_stmt, vec![]).await?;
				}
			}

			Ok(())
		}.boxed_local()
	}
}
