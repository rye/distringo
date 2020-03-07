use crate::error::Result;
use crate::parser::common::LogicalRecord;
use std::path::Path;

impl Dataset {
	pub fn load<Pa: std::fmt::Display + AsRef<Path>> (
		schema: crate::schema::GeographicalHeaderSchema,
		packing_list: Pa,
		geographical_header: Pa,
		files: Vec<Pa>,
	) -> Result<Self> {
		let data: std::collections::BTreeSet<LogicalRecord> = Self::load_data(packing_list, geographical_header, files)?;


		Ok(Dataset {
			schema,
			data,
		})
	}

	fn parse_geographical_header_line(
		schema: &crate::schema::GeographicalHeaderSchema,
		line: String,
	) -> Result<LogicalRecord> {
		match schema {
			crate::schema::GeographicalHeaderSchema::Census2010 => {
				let number: usize = line[crate::parser::fields::census2010::pl94_171::geographical_header::LOGRECNO].parse()?;
				let name: String = line[crate::parser::fields::census2010::pl94_171::geographical_header::NAME]
					.trim()
					.to_string();

				Ok(LogicalRecord {
					number,
					name,
					header: line,
					records: Vec::new(),
				})
			}
		}
	}

	fn parse_geographic_header(
		schema: crate::schema::GeographicalHeaderSchema,
		stream: impl Iterator<Item = std::io::Result<String>> + 'static,
	) -> impl Iterator<Item = Result<LogicalRecord>> + 'static {
		stream
			.filter_map(std::io::Result::ok)
			.map(move |line: String| -> Result<LogicalRecord> {
				Self::parse_geographical_header_line(&schema, line)
			})
	}

	fn load_data<P: AsRef<Path> + core::fmt::Display>(
		packing_list: P,
		header: P,
		_data_files: Vec<P>,
	) -> Result<std::collections::BTreeSet<LogicalRecord>> {
		let _packing_list: crate::parser::packing_list::PackingList =
			crate::parser::packing_list::PackingList::from_file(packing_list)?;

		// A dataset _is_ a BTreeSet because it does have some order.
		let dataset = {
			log::debug!("Loading header file {}", header);
			let file: std::fs::File = std::fs::File::open(header)?;
			let reader = std::io::BufReader::new(file);
			use std::io::BufRead;
			// TODO parse out schema information from PL
			Self::parse_geographic_header(
				crate::schema::GeographicalHeaderSchema::Census2010,
				reader.lines(),
			)
			.filter_map(Result::ok)
			.collect()
		};

		Ok(dataset)
	}

}

pub struct Dataset {
	schema: crate::schema::GeographicalHeaderSchema,
	// packing_list: Pa,
	// geographical_header: Pa,
	// files: Vec<Pa>,
	data: std::collections::btree_set::BTreeSet<LogicalRecord>,
}

