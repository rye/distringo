fn main() -> uptown::error::Result<()> {
	simple_logger::init_with_level(log::Level::Trace).unwrap();

	let ds = uptown::Dataset::read_packing_list("data/in2010.pl.prd.packinglist.txt")?;
	ds.generate_index()?;

	println!("{:?}", ds.get_logical_record(0335180)?);

	Ok(())
}
