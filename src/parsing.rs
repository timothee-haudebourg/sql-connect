use std::iter::Peekable;
use std::str::CharIndices;

pub struct Statements<'a> {
	sql: &'a str,
	chars: Peekable<CharIndices<'a>>,
	offset: usize,
	state: Vec<State>
}

#[derive(Clone, Copy)]
enum State {
	Group,
	String
}

impl<'a> Iterator for Statements<'a> {
	type Item = &'a str;

	fn next(&mut self) -> Option<&'a str> {
		while let Some((i, c)) = self.chars.next() {
			match self.state.last() {
				None => match c {
					';' => {
						let stmt = unsafe { self.sql.get_unchecked(self.offset..i) };
						self.offset = i + 1;
						return Some(stmt)
					},
					'\'' => {
						self.state.push(State::String)
					},
					'(' => {
						self.state.push(State::Group)
					},
					_ => ()
				},
				Some(State::Group) => match c {
					')' => {
						self.state.pop();
					},
					'(' => {
						self.state.push(State::Group)
					},
					'\'' => {
						self.state.push(State::String)
					},
					_ => ()
				},
				Some(State::String) => match c {
					'\'' => {
						match self.chars.peek() {
							Some((c, '\'')) => {
								self.chars.next(); // skip the next quote.
							},
							_ => {
								self.state.pop();
							}
						}
					},
					_ => ()
				}
			}
		}

		if self.offset < self.sql.len() {
			let stmt = unsafe { self.sql.get_unchecked(self.offset..self.sql.len()) };
			self.offset = self.sql.len();
			Some(stmt)
		} else {
			None
		}
	}
}

pub fn split_statement_list(sql: &str) -> Statements {
	Statements {
		sql,
		chars: sql.char_indices().peekable(),
		offset: 0,
		state: Vec::new()
	}
}

#[cfg(test)]
mod tests {
	use super::split_statement_list;

	#[test]
	fn single() {
		let mut statements = split_statement_list("A");
		assert_eq!(statements.next(), Some("A"));
		assert_eq!(statements.next(), None);
	}

	#[test]
	fn single_with_semicolon() {
		let mut statements = split_statement_list("A;");
		assert_eq!(statements.next(), Some("A"));
		assert_eq!(statements.next(), None);
	}

	#[test]
	fn single_with_semicolon_and_space() {
		let mut statements = split_statement_list("A;  ");
		assert_eq!(statements.next(), Some("A"));
		assert_eq!(statements.next(), Some("  "));
		assert_eq!(statements.next(), None);
	}

	#[test]
	fn multi() {
		let mut statements = split_statement_list("A; B; CDEF; GHI;      J");
		assert_eq!(statements.next(), Some("A"));
		assert_eq!(statements.next(), Some(" B"));
		assert_eq!(statements.next(), Some(" CDEF"));
		assert_eq!(statements.next(), Some(" GHI"));
		assert_eq!(statements.next(), Some("      J"));
		assert_eq!(statements.next(), None);
	}

	#[test]
	fn with_string() {
		let mut statements = split_statement_list("A 'foo''b;ar' B; C");
		assert_eq!(statements.next(), Some("A 'foo''b;ar' B"));
		assert_eq!(statements.next(), Some(" C"));
		assert_eq!(statements.next(), None);
	}

	#[test]
	fn with_group() {
		let mut statements = split_statement_list("A (foo; bar) B; C");
		assert_eq!(statements.next(), Some("A (foo; bar) B"));
		assert_eq!(statements.next(), Some(" C"));
		assert_eq!(statements.next(), None);
	}
}
