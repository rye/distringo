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
		.header_file(data_directory.join("ingeo2010.pl.trim"))?
		.tabular_file(1, data_directory.join("in000012010.pl.trim"))?
		.tabular_file(2, data_directory.join("in000022010.pl.trim"))?
		.tabular_file(3, data_directory.join("in000032010.pl.trim-i-do-not-exist"))?
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
		.header_file(data_directory.join("ingeo2010.pl.trim"))?
		.tabular_file(1, data_directory.join("in000012010.pl.trim"))?
		.tabular_file(2, data_directory.join("in000022010.pl.trim"))?
		.index()
		.is_ok());

	Ok(())
}
