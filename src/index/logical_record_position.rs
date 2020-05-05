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

impl Extend<(LogicalRecordNumber, u64)> for LogicalRecordPositionIndex {
	fn extend<T>(&mut self, tup: T)
	where
		T: IntoIterator<Item = (LogicalRecordNumber, u64)>,
	{
		tup.into_iter().for_each(|tup: (LogicalRecordNumber, u64)| {
			self.inner[tup.0 as usize] = tup.1;
		})
	}
}

impl core::ops::Index<LogicalRecordNumber> for LogicalRecordPositionIndex {
	type Output = u64;
	fn index(&self, logrecno: LogicalRecordNumber) -> &u64 {
		&self.inner[logrecno as usize]
	}
}
