use crate::LogicalRecordNumber;

pub struct LogicalRecordPositionIndex {
	inner: Vec<Option<u64>>,
}

impl LogicalRecordPositionIndex {
	pub fn new_with_size(size: usize) -> Self {
		Self {
			inner: Vec::with_capacity(size),
		}
	}

	pub fn insert(&mut self, logrecno: LogicalRecordNumber, offset: u64) {
		let idx: usize = logrecno as usize;
		self.inner.resize(idx + 1, None);
		self.inner[idx] = Some(offset);
	}
}

impl core::ops::Index<LogicalRecordNumber> for LogicalRecordPositionIndex {
	type Output = Option<u64>;
	fn index(&self, logrecno: LogicalRecordNumber) -> &Option<u64> {
		&self.inner[logrecno as usize]
	}
}
