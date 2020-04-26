use fnv::FnvHashMap;

use std::collections::BTreeMap;

mod error;
pub use error::*;

pub type LogicalRecordNumber = u64;
pub type GeoId = String;

mod index;
pub use index::*;

/// A Logical Record
pub trait LogicalRecord {
	/// Get the corresponding number
	///
	/// The Census refers to records by their "logical number."  A logical record
	/// is assumed _only_ to have this number.
	fn number(&self) -> LogicalRecordNumber;
}

mod dataset;
pub use dataset::*;

pub struct FileBackedLogicalRecord {
	number: LogicalRecordNumber,
	records: FnvHashMap<usize, csv::StringRecord>,
}

impl LogicalRecord for FileBackedLogicalRecord {
	fn number(&self) -> LogicalRecordNumber {
		self.number
	}
}

impl FileBackedLogicalRecord {
	fn new(number: LogicalRecordNumber) -> Self {
		Self {
			number,

			records: FnvHashMap::default(),
		}
	}

	fn records(mut self, records: BTreeMap<usize, csv::StringRecord>) -> Self {
		self.records.extend(records);
		self
	}
}

/// A geographical header
pub trait GeographicalHeader {
	fn name(&self) -> &str;
	fn logrecno(&self) -> LogicalRecordNumber;
}

pub mod census2010;

mod schema;
pub use schema::*;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
#[deprecated]
pub(crate) enum FileType {
	Census2010Pl94_171(census2010::pl94_171::FileType),
}

impl FileType {
	fn is_header(&self) -> bool {
		match self {
			Self::Census2010Pl94_171(census2010::pl94_171::FileType::GeographicalHeader) => true,
			_ => false,
		}
	}

	fn is_tabular(&self) -> bool {
		match self {
			Self::Census2010Pl94_171(census2010::pl94_171::FileType::Tabular(_)) => true,
			_ => false,
		}
	}

	fn tabular_index(&self) -> Option<usize> {
		match self {
			Self::Census2010Pl94_171(census2010::pl94_171::FileType::Tabular(n)) => Some(*n),
			_ => None,
		}
	}
}
pub(crate) type GeographicalHeaderIndex = BTreeMap<GeoId, (LogicalRecordNumber, u64)>;
pub(crate) type LogicalRecordIndex = FnvHashMap<FileType, LogicalRecordPositionIndex>;

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct TableSegmentSpecifier {
	file: usize,
	columns: usize,
}

#[derive(Clone, Debug)]
pub struct TableSegmentLocation {
	file: usize,
	range: core::ops::Range<usize>,
}

pub type TableName = String;
pub type TableLocationSpecifier = Vec<TableSegmentSpecifier>;
pub type TableLocations = Vec<TableSegmentLocation>;
