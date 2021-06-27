use std::path::PathBuf;

fn data_directory() -> PathBuf {
	PathBuf::from(file!())
		.parent()
		.expect("could not get parent directory of current file... what?")
		.join("data")
}

#[test]
fn non_existent_tabular_file_errors() -> anyhow::Result<()> {
	let data_directory = data_directory();

	assert!(distringo::Dataset::new()
		.header_file(data_directory.join("rigeo2018.pl.trim"))?
		.tabular_file(1, data_directory.join("ri000012018_2020Style.pl.trim"))?
		.tabular_file(2, data_directory.join("ri000022018_2020Style.pl.trim"))?
		.tabular_file(3, data_directory.join("ri000032018_2020Style.pl.trim"))?
		.tabular_file(4, data_directory.join("ri000042018_2020Style.pl.trim"))?
		.index()
		.is_err());

	Ok(())
}

#[test]
fn main() -> anyhow::Result<()> {
	simple_logger::SimpleLogger::new()
		.with_level(log::LevelFilter::Trace)
		.init()
		.unwrap();

	let data_directory = data_directory();

	assert!(distringo::Dataset::new()
		.header_file(data_directory.join("rigeo2018_2020Style.pl.trim"))?
		.tabular_file(1, data_directory.join("ri000012018_2020Style.pl.trim"))?
		.tabular_file(2, data_directory.join("ri000022018_2020Style.pl.trim"))?
		.tabular_file(3, data_directory.join("ri000022018_2020Style.pl.trim"))?
		.index()
		.is_ok());

	Ok(())
}
