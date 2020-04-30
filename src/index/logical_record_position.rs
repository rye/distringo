use crate::LogicalRecordNumber;

pub struct LogicalRecordPositionIndex {
	inner: Vec<u64>,
}

impl LogicalRecordPositionIndex {
	pub fn new_with_size(size: usize) -> Self {
		let inner = vec![0_u64; size];

		Self { inner }
	}

	pub fn insert(&mut self, logrecno: LogicalRecordNumber, offset: u64) {
		let idx: usize = logrecno as usize;
		self.inner[idx] = offset;
	}
}

impl core::ops::Index<LogicalRecordNumber> for LogicalRecordPositionIndex {
	type Output = u64;
	fn index(&self, logrecno: LogicalRecordNumber) -> &u64 {
		&self.inner[logrecno as usize]
	}
}
