extern crate async_std;
extern crate sql_connect;
use futures::stream::{
	StreamExt,
	Stream
};

use sql_connect::{
	Connection,
	Transaction,
	TransactionCapable
};

#[async_std::test]
async fn connect() -> sql_connect::Result<()> {
	sql_connect::sqlite::Connection::new()?;
	Ok(())
}

#[async_std::test]
async fn prepare() -> sql_connect::Result<()> {
	let mut ctx = sql_connect::sqlite::Connection::new()?;
	ctx.prepare("CREATE TABLE foo (id TEXT PRIMARY KEY);")?;
	Ok(())
}

#[async_std::test]
async fn prepare_empty() -> sql_connect::Result<()> {
	let mut ctx = sql_connect::sqlite::Connection::new()?;
	assert!(ctx.prepare(" ")?.is_none());
	Ok(())
}

#[async_std::test]
async fn create_table() -> sql_connect::Result<()> {
	let mut ctx = sql_connect::sqlite::Connection::new()?;

	let stmt = ctx.prepare("CREATE TABLE foo (id TEXT PRIMARY KEY)")?.unwrap();
	assert!(ctx.execute::<()>(&stmt, vec![]).await?.is_none());

	let stmt = ctx.prepare("INSERT INTO foo (id) VALUES ('bar')")?.unwrap();
	assert!(ctx.execute::<()>(&stmt, vec![]).await?.is_none());

	let stmt = ctx.prepare("SELECT (id) FROM foo")?.unwrap();
	let mut rows = ctx.execute::<String>(&stmt, vec![]).await?.unwrap();
 	let mut rows: Vec<_> = rows.collect().await;

	assert_eq!(rows.len(), 1);
	assert_eq!(rows.into_iter().next().unwrap()?, "bar");
	Ok(())
}

#[async_std::test]
async fn transaction() -> sql_connect::Result<()> {
	let mut ctx = sql_connect::sqlite::Connection::new()?;

	let stmt = ctx.prepare("CREATE TABLE foo (id TEXT PRIMARY KEY)")?.unwrap();
	assert!(ctx.execute::<()>(&stmt, vec![]).await?.is_none());

	let stmt = ctx.prepare("INSERT INTO foo (id) VALUES ('bar')")?.unwrap();
	assert!(ctx.execute::<()>(&stmt, vec![]).await?.is_none());

	let mut trans = ctx.begin().await?;

	let stmt = trans.prepare("INSERT INTO foo (id) VALUES ('biz')")?.unwrap();
	assert!(trans.execute::<()>(&stmt, vec![]).await?.is_none());

	let stmtt = trans.prepare("SELECT (id) FROM foo")?.unwrap();
	let mut rows = trans.execute::<String>(&stmtt, vec![]).await?.unwrap();
 	let mut rows: Vec<_> = rows.collect().await;
	assert_eq!(rows.len(), 2);

	trans.rollback().await?;

	let stmtt = trans.prepare("SELECT (id) FROM foo")?.unwrap();
	let mut rows = trans.execute::<String>(&stmtt, vec![]).await?.unwrap();
 	let mut rows: Vec<_> = rows.collect().await;
	assert_eq!(rows.len(), 1);

	Ok(())
}

// #[async_std::test]
// async fn nested_transaction() -> sql_connect::Result<()> {
// 	let mut ctx = sql_connect::sqlite::Connection::new()?;
//
// 	let mut trans = ctx.begin().await?;
// 	let mut nested = trans.begin().await?;
// 	// ...
// 	nested.commit().await?;
// 	trans.commit().await?;
//
// 	Ok(())
// }
