use std::pin::Pin;
use std::task::{
	Poll,
	Context
};
use futures::{
	stream::Stream
};
use crate::{
	Value,
	FromValue,
	Result
};

/// Types that can be converted from a data column.
pub trait FromRow: Sized {
	fn from<'a, R: Iterator<Item = Value<'a>>>(row: R) -> Self;
}

/// Convert a single-column row into the given type.
///
/// The convertion will panic if the row is empty, or if the convertion from column value panics.
impl<T> FromRow for T where T: FromValue {
	fn from<'a, R: Iterator<Item = Value<'a>>>(mut row: R) -> T {
		T::from(row.next().unwrap())
	}
}

macro_rules! tuple_from_row {
	( $( $t:tt ),+ ) => {
		/// Convert a n-column row into the given n-uplet.
		///
		/// The convertion will panic if the row is too short,
		/// or if the convertion from column value panics.
		impl < $( $t, )* > FromRow for ( $( $t ),* ) where $( $t: FromValue, )+ {
			fn from<'a, R: Iterator<Item = Value<'a>>>(mut row: R) -> ( $( $t ),* ) {
				($( $t::from(row.next().unwrap()), )*)
			}
		}
	};
}

tuple_from_row!(T1, T2);
tuple_from_row!(T1, T2, T3);
tuple_from_row!(T1, T2, T3, T4);
tuple_from_row!(T1, T2, T3, T4, T5);
tuple_from_row!(T1, T2, T3, T4, T5, T6);
tuple_from_row!(T1, T2, T3, T4, T5, T6, T7);
tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8);

pub struct Rows<'a, R> {
	inner: Pin<Box<dyn 'a + Stream<Item = Result<R>>>>
}

impl<'a, R> Rows<'a, R> {
	pub fn new<S: 'a + Stream<Item = Result<R>>>(rows: S) -> Rows<'a, R> {
		Rows {
			inner: Box::pin(rows)
		}
	}
}

impl<'a, R> Stream for Rows<'a, R> {
	type Item = Result<R>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
		self.inner.as_mut().poll_next(cx)
	}
}
