use csv::StringRecord;
use crate::error::Result;
use crate::parser::common::{LogicalRecord, LogicalRecordNumber};
use crate::parser::packing_list::PackingList;
use crate::parser::packing_list::SegmentationInformation;
use crate::parser::packing_list::SegmentedFileIndex;
use crate::schema::CensusDataSchema;
use core::ops::Range;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub struct Dataset {
	schema: CensusDataSchema,
	data: HashMap<LogicalRecordNumber, LogicalRecord>,
}

impl Dataset {
	pub fn load<P: AsRef<Path>>(packing_list_file: P, tables_to_load: &Vec<String>) -> Result<Self> {
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
					records: HashMap::new(),
				};

				data.insert(number, record);
			}
		}

		let tables: &Vec<(String, SegmentationInformation)> = packing_list.tables();
		let tabular_files: BTreeMap<SegmentedFileIndex, PathBuf> = packing_list.tabular_files();
		let mut table_locations: BTreeMap<String, Vec<(SegmentedFileIndex, Range<usize>)>> =
			BTreeMap::new();

		{
			let mut current_columns: HashMap<SegmentedFileIndex, usize> = tabular_files
				.iter()
				.map(|(fidx, _path)| -> (SegmentedFileIndex, usize) { (*fidx, 5) })
				.collect();

			for (table, segmentation_information) in tables {
				log::debug!(
					"Segmentation information for {}: {:?}",
					table,
					segmentation_information
				);

				let locations: Vec<(SegmentedFileIndex, Range<usize>)> = segmentation_information
					.file_width
					.iter()
					.map(|(sidx, width)| -> (SegmentedFileIndex, Range<usize>) {
						let start: usize = *current_columns.get(sidx).expect(&format!(
							"failed to find segmented file with index {}",
							sidx
						));
						let end: usize = start + width;
						let range = start..end;

						current_columns.insert(*sidx, end);

						(*sidx, range)
					})
					.collect();

				table_locations.insert(table.to_string(), locations.clone());

				log::info!("Table locations for {}: {:?}", table, locations);
			}
		}

		{
			let mut raw_data: HashMap<SegmentedFileIndex, HashMap<LogicalRecordNumber, Vec<String>>> = HashMap::new();

			let paths_to_load: HashMap<SegmentedFileIndex, PathBuf> = tables_to_load.iter().flat_map(|table| -> Vec<SegmentedFileIndex> {
				table_locations.get(table).expect("couldn't locate table").iter().map(|segs| segs.0).collect()
			}).map(|sidx| -> (SegmentedFileIndex, PathBuf) {
				(sidx, tabular_files.get(&sidx).expect("invalid sidx").clone())
			}).collect();

			for (sidx, path) in paths_to_load {
				log::debug!("Loading file {:?}", path);

				let file: std::fs::File = std::fs::File::open(path)?;
				let mut reader = csv::ReaderBuilder::new().from_reader(file);

				let records: HashMap<LogicalRecordNumber, Vec<String>> = reader.records().map(|record| -> Result<(LogicalRecordNumber, Vec<String>)> {
					let record: StringRecord = record?;
					let number: LogicalRecordNumber = record[4].parse()?;
					let record: Vec<String> = record.into_iter().map(|s| s.to_string()).collect();

					Ok((number, record))
				}).flatten().collect();

				raw_data.insert(sidx, records);
			}

			log::debug!("Cross-associating records");

			for table in tables_to_load {
				log::debug!("Loading table {}", table);
				for locations in table_locations.get(table) {
					log::debug!("{}: Loading from locations {:?}", table, locations);

					for (sidx, location) in locations {
						log::debug!("{}: Loading from file {} at {:?}", table, sidx, location);

						let file: &HashMap<LogicalRecordNumber, Vec<String>> = raw_data.get(sidx).expect("missing raw data");

						for (logrecno, fields) in file.iter() {
							let location: Range<usize> = location.clone();
							let (logrecno, fields): (&LogicalRecordNumber, &Vec<String>) = (logrecno, fields);
							let record: &mut LogicalRecord = data.get_mut(logrecno).expect("cannot add data to missing logical record");
							let tabular_data = &fields[location];

							record.records.insert(table.to_string(), tabular_data.to_vec());
						}
					}
				}
			}
		}

		Ok(Dataset { schema, data })
	}
}
