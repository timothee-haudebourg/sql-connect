use mown::Mown;

pub enum Value<'a> {
	Integer(i64),
	Float(f64),
	Text(Mown<'a, str>),
	Blob(Mown<'a, [u8]>),
	Null
}

pub trait FromValue: Sized {
	fn from<'a>(value: Value<'a>) -> Self;
}

impl FromValue for () {
	fn from<'a>(value: Value<'a>) -> Self {
		()
	}
}

impl FromValue for String {
	fn from<'a>(value: Value<'a>) -> Self {
		match value {
			Value::Text(Mown::Borrowed(str)) => str.to_string(),
			Value::Text(Mown::Owned(str)) => str,
			_ => panic!("invalid convertion")
		}
	}
}
