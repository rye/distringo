use distringo::Dataset;

/// Simple loading example
///
/// Reads a packing list from
fn main() -> distringo::Result<()> {
	simple_logger::init_with_level(log::Level::Trace).unwrap();

	let ds = distringo::IndexedDataset::from_packing_list_file("data/in2010.pl.prd.packinglist.txt")?
		.index()?;

	let logrecno: distringo::LogicalRecordNumber = 335180;

	let record = ds.get_logical_record(logrecno)?;

	let rec_a: Vec<&str> = vec![
		"PLST", "IN", "000", "01", "0335180", "53", "52", "50", "0", "0", "2", "0", "0", "1", "1", "0",
		"0", "1", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "53", "2", "51", "50", "48", "0", "0", "2", "0", "0", "1", "1", "0", "0", "1",
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0",
	];
	let rec_b: Vec<&str> = vec![
		"PLST", "IN", "000", "02", "0335180", "45", "45", "43", "0", "0", "2", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "45", "1", "44", "44", "42", "0", "0", "2", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "24", "24", "0",
	];

	let rec_a: csv::StringRecord = rec_a.into();
	let rec_b: csv::StringRecord = rec_b.into();

	assert_eq!(
		record
			.raw_records()
			.values()
			.collect::<Vec<&csv::StringRecord>>(),
		vec![&rec_a, &rec_b]
	);

	let logrecno = ds.get_logical_record_number_for_geoid("181570052001013")?;
	assert_eq!(logrecno, 335180);

	let header = ds.get_header_for_geoid("181570052001013")?;
	assert_eq!(header.name(), "Block 1013");
	assert_eq!(header.logrecno(), 335180);

	Ok(())
}
