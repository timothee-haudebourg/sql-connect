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

impl FromValue for u32 {
	fn from<'a>(value: Value<'a>) -> Self {
		match value {
			Value::Integer(i) if i >= 0 => i as u32,
			_ => panic!("invalid convertion")
		}
	}
}

impl FromValue for i32 {
	fn from<'a>(value: Value<'a>) -> Self {
		match value {
			Value::Integer(i) => i as i32,
			_ => panic!("invalid convertion")
		}
	}
}

impl FromValue for u64 {
	fn from<'a>(value: Value<'a>) -> Self {
		match value {
			Value::Integer(i) if i >= 0 => i as u64,
			_ => panic!("invalid convertion")
		}
	}
}

impl FromValue for i64 {
	fn from<'a>(value: Value<'a>) -> Self {
		match value {
			Value::Integer(i) => i as i64,
			_ => panic!("invalid convertion")
		}
	}
}

impl FromValue for f32 {
	fn from<'a>(value: Value<'a>) -> Self {
		match value {
			Value::Float(f) => f as f32,
			_ => panic!("invalid convertion")
		}
	}
}

impl FromValue for f64 {
	fn from<'a>(value: Value<'a>) -> Self {
		match value {
			Value::Float(f) => f,
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

impl FromValue for chrono::NaiveDate {
	fn from<'a>(value: Value<'a>) -> Self {
		match value {
			Value::Text(str) => chrono::NaiveDate::parse_from_str(&str, "%Y-%m-%d").unwrap(),
			_ => panic!("invalid convertion")
		}
	}
}

impl FromValue for chrono::NaiveTime {
	fn from<'a>(value: Value<'a>) -> Self {
		match value {
			Value::Text(str) => chrono::NaiveTime::parse_from_str(&str, "%H:%M:%S%.f").unwrap(),
			_ => panic!("invalid convertion")
		}
	}
}

impl FromValue for chrono::NaiveDateTime {
	fn from<'a>(value: Value<'a>) -> Self {
		match value {
			Value::Text(str) => chrono::NaiveDateTime::parse_from_str(&str, "%+").unwrap(),
			_ => panic!("invalid convertion")
		}
	}
}

impl<T: FromValue> FromValue for Option<T> {
	fn from<'a>(value: Value<'a>) -> Self {
		match value {
			Value::Null => None,
			some => Some(T::from(some))
		}
	}
}

impl<'a> From<usize> for Value<'a> {
	fn from(i: usize) -> Value<'a> {
		Value::Integer(i as i64)
	}
}

impl<'a> From<u64> for Value<'a> {
	fn from(i: u64) -> Value<'a> {
		Value::Integer(i as i64)
	}
}

impl<'a> From<i64> for Value<'a> {
	fn from(i: i64) -> Value<'a> {
		Value::Integer(i)
	}
}

impl<'a> From<u32> for Value<'a> {
	fn from(i: u32) -> Value<'a> {
		Value::Integer(i as i64)
	}
}

impl<'a> From<i32> for Value<'a> {
	fn from(i: i32) -> Value<'a> {
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
		Value::Text(Mown::Owned(date.format("%Y-%m-%d").to_string()))
	}
}

impl<'a> From<chrono::NaiveTime> for Value<'a> {
	fn from(date: chrono::NaiveTime) -> Value<'a> {
		Value::Text(Mown::Owned(date.format("%H:%M:%S%.f").to_string()))
	}
}

impl<'a> From<chrono::NaiveDateTime> for Value<'a> {
	fn from(date: chrono::NaiveDateTime) -> Value<'a> {
		Value::Text(Mown::Owned(date.format("%+").to_string()))
	}
}