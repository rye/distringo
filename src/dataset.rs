use crate::error::Result;
use crate::GeographicalHeader;
use crate::LogicalRecordNumber;

/// A trait containing behavior expected for datasets
pub trait Dataset<LogicalRecord> {
	/// Retrieve the logical record with number `number`
	fn get_logical_record(&self, number: LogicalRecordNumber) -> Result<LogicalRecord>;

	/// Retrieve the logical record corresponding to GeoID `id`
	fn get_logical_record_number_for_geoid(&self, geoid: &str) -> Result<LogicalRecordNumber>;

	/// Retrieve the GeographicalHeader
	fn get_header_for_geoid(&self, geoid: &str) -> Result<Box<dyn GeographicalHeader>>;
}

mod indexed;
pub use indexed::*;

mod packing_list;
pub use packing_list::*;
