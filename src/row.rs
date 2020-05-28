use crate::{
	Value,
	FromValue
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
