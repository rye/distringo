use core::ops::Range;

use std::collections::HashMap;

#[derive(PartialEq, Eq)]
pub struct LogicalRecord {
	pub(crate) number: LogicalRecordNumber,
	pub(crate) header: String,
	pub(crate) records: HashMap<String, Record>,
}

// TODO change out &'static str from something that's generated at compile-time according to definitions?
type Record = Vec<String>;

pub type LogicalRecordNumber = usize;

impl PartialOrd for LogicalRecord {
	fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
		// N.B.: PartialOrd for usize _always_ returns Some(something)
		self.number.partial_cmp(&other.number)
	}
}

impl Ord for LogicalRecord {
	fn cmp(&self, other: &Self) -> core::cmp::Ordering {
		// N.B.: PartialOrd for usize _always_ returns Some(something), so just return Equal in the null case.
		self
			.partial_cmp(other)
			.expect("usize comparison via PartialOrd must always return Some")
	}
}

impl LogicalRecord {
	#[allow(dead_code)]
	fn header_field(&self, range: Range<usize>) -> &str {
		&self.header[range]
	}
}
