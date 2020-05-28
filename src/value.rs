pub enum Value<'a> {
	Integer(i64),
	Float(f64),
	Text(&'a str),
	Blob(&'a [u8]),
	Null
}

pub trait FromValue: Sized {
	fn from<'a>(value: Value<'a>) -> Self;
}
