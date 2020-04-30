use fnv::FnvHashMap;

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
	raw_records: FnvHashMap<u32, csv::StringRecord>,
}

impl LogicalRecord for FileBackedLogicalRecord {
	fn number(&self) -> LogicalRecordNumber {
		self.number
	}
}

impl FileBackedLogicalRecord {
	pub fn new(number: LogicalRecordNumber, raw_records: FnvHashMap<u32, csv::StringRecord>) -> Self {
		Self {
			number,
			raw_records,
		}
	}

	pub fn raw_records(&self) -> &FnvHashMap<u32, csv::StringRecord> {
		&self.raw_records
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

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct TableSegmentSpecifier {
	file: u32,
	columns: usize,
}

impl core::str::FromStr for TableSegmentSpecifier {
	type Err = crate::Error;
	fn from_str(s: &str) -> Result<Self> {
		let components: Vec<&str> = s.split(':').collect();
		let file: u32 = components
			.get(0)
			.expect("missing file identifier")
			.parse()
			.expect("couldn't parse file identifier");
		let columns: usize = components
			.get(1)
			.expect("missing column width")
			.parse()
			.expect("couldn't parse column width");

		Ok(Self { file, columns })
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableSegmentLocation {
	file: u32,
	range: core::ops::Range<usize>,
}

pub type TableName = String;
pub type TableLocationSpecifier = Vec<TableSegmentSpecifier>;
pub type TableLocations = Vec<TableSegmentLocation>;
