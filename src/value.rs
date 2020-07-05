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
	fn from<'a>(_value: Value<'a>) -> Self {
		()
	}
}

impl FromValue for usize {
	fn from<'a>(value: Value<'a>) -> Self {
		match value {
			Value::Integer(i) if i >= 0 => i as usize,
			_ => panic!("invalid convertion")
		}
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

impl<'a> From<usize> for Value<'a> {
	fn from(i: usize) -> Value<'a> {
		Value::Integer(i as i64)
	}
}

impl<'a> From<String> for Value<'a> {
	fn from(str: String) -> Value<'a> {
		Value::Text(Mown::Owned(str))
	}
}

impl<'a> From<&'a str> for Value<'a> {
	fn from(str: &'a str) -> Value<'a> {
		Value::Text(Mown::Borrowed(str))
	}
}

impl<'a> From<chrono::NaiveDate> for Value<'a> {
	fn from(date: chrono::NaiveDate) -> Value<'a> {
		Value::Text(Mown::Owned(date.format("%Y-%m-%d")))
	}
}

impl<'a> From<chrono::NaiveTime> for Value<'a> {
	fn from(date: chrono::NaiveDateTime) -> Value<'a> {
		Value::Text(Mown::Owned(date.format("%H:%M:%S%.f")))
	}
}

impl<'a> From<chrono::NaiveDateTime> for Value<'a> {
	fn from(date: chrono::NaiveDateTime) -> Value<'a> {
		Value::Text(Mown::Owned(date.format("%+")))
	}
}
