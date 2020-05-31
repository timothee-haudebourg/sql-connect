use std::mem::MaybeUninit;
use futures::{
	future::{
		LocalBoxFuture,
		FutureExt
	}
};
use mown::Mown;
use crate::{
	Connection,
	Result,
	Statement,
	FromRow,
	Value,
	Rows
};

pub trait TransactionCapable: Connection {
	/// Begin a new toplevel transaction.
	///
	/// This will execute a `BEGIN TRANSACTION` statement.
	fn begin(&mut self) -> LocalBoxFuture<Result<Transaction<Self>>> {
		async move {
			let begin = self.prepare("BEGIN")?.unwrap();
			let end = self.prepare("COMMIT")?.unwrap();
			let rollback = self.prepare("ROLLBACK")?.unwrap();

			begin.execute::<()>(self, vec![]).await?;
			Ok(Transaction {
				connection: self,
				done: false,
				end: Some(end),
				rollback: Some(rollback)
			})
		}.boxed_local()
	}
}

pub trait SavepointCapable: Connection {
	/// Generate a unique savepoint name.
	fn anonymous_savepoint_name(&mut self) -> String;

	/// Begin a new transaction by creating a savepoint.
	///
	/// This will usually execute a `SAVEPOINT name` statement.
	/// If no savepoint name is provided, one will be automatically generated.
	fn savepoint(&mut self, name: Option<String>) -> LocalBoxFuture<Result<Transaction<Self>>> {
		let release = match name {
			Some(name) => format!("RELEASE {}", name),
			None => format!("RELEASE {}", self.anonymous_savepoint_name())
		};

		async move {
			let begin = self.prepare("SAVEPOINT")?.unwrap();
			let end = self.prepare(&release)?.unwrap();
			let rollback = self.prepare("ROLLBACK TO ")?.unwrap();

			begin.execute::<()>(self, vec![]).await?;
			Ok(Transaction {
				connection: self,
				done: false,
				end: Some(end),
				rollback: Some(rollback)
			})
		}.boxed_local()
	}
}

pub struct Transaction<'a, C: Connection> {
	connection: &'a mut C,
	done: bool,
	end: Option<C::Statement>,
	rollback: Option<C::Statement>
}

impl<'a, C: Connection> Connection for Transaction<'a, C> where C::Statement: Statement<Transaction<'a, C>> {
	type Statement = C::Statement;

	fn prepare(&mut self, sql: &str) -> Result<Option<Self::Statement>> {
		self.connection.prepare(sql)
	}

	fn prepare_list(&mut self, sql: &str) -> Result<Vec<Self::Statement>> {
		self.connection.prepare_list(sql)
	}
}

impl<'a, C: SavepointCapable> SavepointCapable for Transaction<'a, C> where C::Statement: Statement<Transaction<'a, C>> {
	fn anonymous_savepoint_name(&mut self) -> String {
		self.connection.anonymous_savepoint_name()
	}

	fn savepoint(&mut self, name: Option<String>) -> LocalBoxFuture<Result<Transaction<Self>>> {
		async move {
			let mut end = None;
			let mut rollback = None;

			{
				let mut trans = self.connection.savepoint(name).await?;
				std::mem::swap(&mut end, &mut trans.end);
				std::mem::swap(&mut rollback, &mut trans.rollback);
			}

			Ok(Transaction {
				connection: self,
				done: false,
				end,
				rollback
			})
		}.boxed_local()
	}
}

impl<'a, C: Connection> Transaction<'a, C> where C::Statement: Statement<Transaction<'a, C>> {
	pub async fn commit(&mut self) -> Result<()> {
		if !self.done {
			self.done = true;
			let mut end = None;
			std::mem::swap(&mut end, &mut self.end);
			if let Some(end) = end {
				end.execute::<()>(self, vec![]).await?;
			}
		}
		Ok(())
	}

	pub async fn rollback(&mut self) -> Result<()> {
		if !self.done {
			self.done = true;
			let mut rollback = None;
			std::mem::swap(&mut rollback, &mut self.rollback);
			if let Some(rollback) = rollback {
				rollback.execute::<()>(self, vec![]).await?;
			}
		}
		Ok(())
	}
}

impl<'c, C: Connection, S: Statement<C>> Statement<Transaction<'c, C>> for S {
	fn execute<'a, R: 'a + FromRow>(&'a self, connection: &mut Transaction<C>, args: Vec<Value>) -> LocalBoxFuture<Result<Option<Rows<'a, R>>>> {
		self.execute(connection.connection, args)
	}
}

impl<'a, C: Connection> Drop for Transaction<'a, C> {
	/// Commit the transaction before dropping it.
	///
	/// This will block the current thread until the transaction is commited,
	/// unless it has already be commited or rolled back explicitelly.
	fn drop(&mut self) {
		if !self.done {
			futures::executor::block_on(self.commit());
		}
	}
}