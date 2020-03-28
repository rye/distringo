use uptown::Dataset;

/// Simple loading example
///
/// Reads a packing list from
fn main() -> uptown::error::Result<()> {
	simple_logger::init_with_level(log::Level::Trace).unwrap();

	let ds = uptown::IndexedPackingListDataset::new("in2010-pl94_171")
		.unpack("data/in2010.pl.prd.packinglist.txt");

	ds.index()?;

	println!("{:?}", ds.get_logical_record(0335180)?);

	Ok(())
}
