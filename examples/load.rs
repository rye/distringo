use distringo::Dataset;

/// Simple loading example
///
/// Reads a packing list from
fn main() -> distringo::Result<()> {
	simple_logger::init_with_level(log::Level::Trace).unwrap();

	let ds = distringo::IndexedDataset::new("in2010-pl94_171")
		.unpack("data/in2010.pl.prd.packinglist.txt")?
		.index()?;

	let logrecno: distringo::LogicalRecordNumber = 0335180;

	let record = ds.get_logical_record(logrecno)?;

	let logrecno = ds.get_logical_record_number_for_geoid("181570052001013")?;
	assert_eq!(logrecno, 0335180);

	let header = ds.get_header_for_geoid("181570052001013")?;
	assert_eq!(header.name(), "Block 1013");

	assert_eq!(header.logrecno(), 0335180);

	Ok(())
}
