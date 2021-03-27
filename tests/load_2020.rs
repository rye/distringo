use distringo::Dataset;

use fnv::FnvHashMap;

/// Simple loading example
///
/// Reads a packing list from
#[test]
fn main() -> distringo::Result<()> {
	simple_logger::init_with_level(log::Level::Trace).unwrap();

	let schema = distringo::Schema::Census2020(distringo::census2020::Schema::Pl94_171);

	let directory = Some(std::path::PathBuf::from("data"));

	let table_locations = {
		let mut map = FnvHashMap::default();

		map.insert(
			distringo::Table::Census2020(distringo::census2020::Table::Pl94_171(
				distringo::census2020::pl94_171::P1,
			)),
			vec![],
		);

		map
	};

	let tabular_files = {
		let mut map = FnvHashMap::default();

		map.insert(0_u32, std::path::PathBuf::from("ri000012018_2020Style.pl"));

		map
	};

	let geographical_header_file = std::path::PathBuf::from("rigeo2018_2020Style.pl");
	let rows = 19346_usize;

	let pl: distringo::PackingList = distringo::PackingList::new(
		schema,
		directory,
		table_locations,
		tabular_files,
		geographical_header_file,
		rows,
	);

	let ds = distringo::IndexedDataset::from_packing_list(pl)?.index()?;

	let logrecno: distringo::LogicalRecordNumber = 335_180;

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
	assert_eq!(logrecno, 335_180);

	let header = ds.get_header_for_geoid("181570052001013")?;
	assert_eq!(header.name(), "Block 1013");
	assert_eq!(header.logrecno(), 335_180);

	Ok(())
}
