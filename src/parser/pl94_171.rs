use crate::error::Result;
use crate::parser::common::LogicalRecord;
use crate::parser::packing_list::PackingList;
use crate::schema::CensusDataSchema;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub struct Dataset {
	schema: CensusDataSchema,
	data: HashMap<usize, LogicalRecord>,
}

impl Dataset {
	pub fn load<P: AsRef<Path>>(packing_list_file: P) -> Result<Self> {
		let packing_list: PackingList = PackingList::from_file(&packing_list_file)?;

		let schema: CensusDataSchema = packing_list.schema();

		let mut data = HashMap::new();

		{
			let header_file = packing_list.header_file();
			let file: std::fs::File = std::fs::File::open(&header_file)?;
			let reader = std::io::BufReader::new(file);

			use std::io::BufRead;

			for line in reader.lines() {
				let line: String = line?.to_string();

				let number: usize = line
					[crate::parser::fields::census2010::pl94_171::geographical_header::LOGRECNO]
					.parse()?;

				let record: LogicalRecord = LogicalRecord {
					number,
					header: line,
					records: Vec::new(),
				};

				data.insert(number, record);
			}
		}

		let tabular_files: BTreeMap<u16, PathBuf> = packing_list.tabular_files();

		Ok(Dataset { schema, data })
	}
}
