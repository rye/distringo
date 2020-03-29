use uptown::Dataset;

/// Simple loading example
///
/// Reads a packing list from
fn main() -> uptown::error::Result<()> {
	simple_logger::init_with_level(log::Level::Trace).unwrap();

	let ds = uptown::IndexedPackingListDataset::new("in2010-pl94_171")
		.unpack("data/in2010.pl.prd.packinglist.txt")?
		.index()?;

	let start = std::time::Instant::now();
	let string_record = ds.get_logical_record(
		0335180,
		vec![
			uptown::Schema::Census2010Pl94_171(Some(uptown::census2010::pl94_171::P1)),
			uptown::Schema::Census2010Pl94_171(Some(uptown::census2010::pl94_171::P2)),
			uptown::Schema::Census2010Pl94_171(Some(uptown::census2010::pl94_171::P3)),
			uptown::Schema::Census2010Pl94_171(Some(uptown::census2010::pl94_171::P4)),
			uptown::Schema::Census2010Pl94_171(Some(uptown::census2010::pl94_171::H1)),
		],
	)?;
	println!(
		"Retrieved record {:?} in {}ns",
		string_record,
		std::time::Instant::now().duration_since(start).as_nanos()
	);

	Ok(())
}
