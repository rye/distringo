use crate::error::Result;
use crate::parser::common::LogicalRecord;
use crate::parser::packing_list::PackingList;
use crate::schema::CensusDataSchema;
use std::collections::BTreeSet;
use std::path::Path;

pub struct Dataset {
	schema: CensusDataSchema,
	data: BTreeSet<LogicalRecord>,
}

impl Dataset {
	pub fn load<Pa: std::fmt::Display + AsRef<Path>>(
		packing_list: Pa,
		geographical_header: Pa,
		files: Vec<Pa>,
	) -> Result<Self> {
		let packing_list: PackingList =
			PackingList::from_file(packing_list)?;
		let schema: CensusDataSchema = packing_list.schema();
		let data: BTreeSet<LogicalRecord> =
			Self::load_data(schema, packing_list, geographical_header, files)?;

		Ok(Dataset { schema, data })
	}

	fn parse_geographical_header_line(
		schema: &CensusDataSchema,
		line: String,
	) -> Result<LogicalRecord> {
		match schema {
			CensusDataSchema::Census2010Pl94_171 => {
				let number: usize = line
					[crate::parser::fields::census2010::pl94_171::geographical_header::LOGRECNO]
					.parse()?;
				let name: String = line
					[crate::parser::fields::census2010::pl94_171::geographical_header::NAME]
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
		schema: CensusDataSchema,
		stream: impl Iterator<Item = std::io::Result<String>> + 'static,
	) -> impl Iterator<Item = Result<LogicalRecord>> + 'static {
		stream
			.filter_map(std::io::Result::ok)
			.map(move |line: String| -> Result<LogicalRecord> {
				Self::parse_geographical_header_line(&schema, line)
			})
	}

	fn load_data<P: AsRef<Path> + core::fmt::Display>(
		schema: CensusDataSchema,
		packing_list: PackingList,
		header: P,
		files: Vec<P>,
	) -> Result<BTreeSet<LogicalRecord>> {
		// A dataset _is_ a BTreeSet because it does have some order.
		let dataset = {
			log::debug!("Loading header file {}", header);

			let file: std::fs::File = std::fs::File::open(header)?;
			let reader = std::io::BufReader::new(file);

			use std::io::BufRead;
			// TODO parse out schema information from PL
			Self::parse_geographic_header(schema, reader.lines())
				.filter_map(Result::ok)
				.collect()
		};

		Ok(dataset)
	}
}
