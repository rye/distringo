use crate::error::Result;
use crate::parser::common::LogicalRecord;
use crate::parser::packing_list::PackingList;
use crate::schema::CensusDataSchema;
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

pub struct Dataset {
	schema: CensusDataSchema,
	data: BTreeSet<LogicalRecord>,
}

impl Dataset {
	pub fn load<Pa: std::fmt::Display + AsRef<Path>>(
		packing_list_file: Pa,
		geographical_header_file: Pa,
		files: Vec<Pa>,
	) -> Result<Self> {
		log::trace!("Loading packing list {}", packing_list_file);

		let start = Instant::now();

		// Load the packing list
		let packing_list: PackingList = PackingList::from_file(&packing_list_file)?;

		log::trace!("Finished loading packing list in {}\u{b5}s", Instant::now().duration_since(start).as_micros());

		// Infer the schema from the packing list
		let schema: CensusDataSchema = packing_list.schema();

		log::debug!("Inferred schema from {}: {:?}", &packing_list_file, schema);

		// Load the data
		// TODO Make this an instance method
		// TODO Load in stages
		// TODO parallelize
		let data: BTreeSet<LogicalRecord> =
			Self::load_data(schema, packing_list, geographical_header_file, files)?;

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
		header_file: P,
		files: Vec<P>,
	) -> Result<BTreeSet<LogicalRecord>> {
		let beginning_of_load = Instant::now();

		log::trace!("Loading data...");

		// A dataset _is_ a BTreeSet because it does have some order.
		let dataset = {
			log::debug!("Loading header file {}", header_file);

			let start = Instant::now();

			let file: std::fs::File = std::fs::File::open(&header_file)?;
			let reader = std::io::BufReader::new(file);

			use std::io::BufRead;

			let header = Self::parse_geographic_header(schema, reader.lines())
				.filter_map(Result::ok)
				.collect();

			log::trace!("Finished loading header file {} in {}\u{b5}s", &header_file, Instant::now().duration_since(start).as_micros());

			header
		};

		log::trace!("Finished loading dataset in {}\u{b5}s", Instant::now().duration_since(beginning_of_load).as_micros());

		Ok(dataset)
	}
}
