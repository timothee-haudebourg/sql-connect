#[macro_use]
extern crate pin_utils;

use futures::{
	Stream,
	future::LocalBoxFuture
};

mod error;
mod backoff;
mod backend;
mod value;
mod row;

pub use error::*;
pub use self::backoff::*;
pub use backend::*;
pub use value::*;
pub use row::*;

pub trait Connection {
	type Statement: Statement;

	fn prepare(sql: &str) -> Self::Statement;
}

pub trait Statement {
	/// Execute the statement.
	/// If the statement is a data query, returns some stream of rows.
	fn execute<'a, Row: 'a + FromRow>(&'a self) -> LocalBoxFuture<Result<Option<Box<dyn 'a + Stream<Item = Result<Row>>>>>>;
}
