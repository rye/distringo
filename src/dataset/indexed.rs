use std::{
	collections::BTreeMap,
	fs::File,
	io::{BufRead, BufReader, Seek},
	path::Path,
};

use crate::{
	census2010, census2020,
	dataset::{packing_list::PackingList, Dataset},
	FileBackedLogicalRecord, GeoId, GeographicalHeader, LogicalRecordNumber,
	LogicalRecordPositionIndex, Result, Schema, Table, TableLocations,
};

use fnv::FnvHashMap;

/// A Census Dataset
///
/// Every dataset has a unique, human-identifiable identifier, which is useds
/// internally for reading the data.
pub struct IndexedDataset {
	schema: Schema,
	header_index: Option<GeographicalHeaderIndex>,
	tabular_index: Option<TabularIndex>,
	table_locations: FnvHashMap<Table, TableLocations>,
	geographical_header: File,
	tabular_files: FnvHashMap<u32, File>,
	rows: usize,
}

pub(crate) type GeographicalHeaderIndex = BTreeMap<GeoId, (LogicalRecordNumber, u64)>;
pub(crate) type TabularIndex = FnvHashMap<u32, LogicalRecordPositionIndex>;

impl Dataset<FileBackedLogicalRecord, LogicalRecordNumber> for IndexedDataset {
	/// Retrieve the logical record by number and by table
	fn get_logical_record(&self, number: LogicalRecordNumber) -> Result<FileBackedLogicalRecord> {
		match &self.tabular_index {
			Some(index) => {
				let records_from_file: FnvHashMap<u32, csv::StringRecord> = self
					.tabular_files
					.iter()
					.map(|(idx, file)| -> (u32, csv::StringRecord) {
						let corresponding_logrec_position_index = index.get(&idx).unwrap();
						let offset: u64 = corresponding_logrec_position_index[number];

						let mut reader = BufReader::new(file);
						reader
							.seek(std::io::SeekFrom::Start(offset))
							.expect("failed to seek to position for record");

						let mut reader = csv::ReaderBuilder::new()
							.has_headers(false)
							.delimiter(match self.schema {
								Schema::Census2010(_) => b',',
								Schema::Census2020(_) => b'|',
							})
							.from_reader(reader);
						let mut record = csv::StringRecord::new();
						reader
							.read_record(&mut record)
							.expect("failed to read record");

						(*idx, record)
					})
					.collect();

				let record = FileBackedLogicalRecord::new(number, records_from_file);

				Ok(record)
			}

			None => unimplemented!(),
		}
	}

	fn get_logical_record_number_for_geoid(&self, geoid: &str) -> Result<u64> {
		if let Some(index) = &self.header_index {
			let result: &(LogicalRecordNumber, u64) =
				index.get(geoid).ok_or(crate::Error::InvalidGeoId)?;

			let logrecno: LogicalRecordNumber = result.0;

			Ok(logrecno)
		} else {
			unimplemented!()
		}
	}

	fn get_header_for_geoid(&self, geoid: &str) -> Result<Box<dyn GeographicalHeader>> {
		if let Some(index) = &self.header_index {
			let result: &(LogicalRecordNumber, u64) = index.get(geoid).unwrap();

			let line_offset = result.1;

			let file = &self.geographical_header;

			let mut reader = BufReader::new(file);

			reader.seek(std::io::SeekFrom::Start(line_offset))?;

			let mut line = String::new();
			reader.read_line(&mut line)?;

			match self.schema {
				Schema::Census2010(census2010::Schema::Pl94_171) => Ok(Box::new(
					crate::census2010::pl94_171::GeographicalHeader::new(line),
				)),

				Schema::Census2020(census2020::Schema::Pl94_171) => Ok(Box::new(
					crate::census2020::pl94_171::GeographicalHeader::new(&line),
				)),
			}
		} else {
			unimplemented!()
		}
	}
}

impl IndexedDataset {
	pub fn from_packing_list(packing_list: PackingList) -> Result<Self> {
		let schema = packing_list.schema();
		let geographical_header: File = File::open(
			packing_list
				.locate(packing_list.geographical_header_file())
				.expect("couldn't locate geographical header file"),
		)?;
		let header_index: Option<GeographicalHeaderIndex> = None;
		let tabular_index: Option<TabularIndex> = None;
		let table_locations: FnvHashMap<Table, TableLocations> = packing_list.table_locations().clone();
		let tabular_files: FnvHashMap<u32, File> = packing_list
			.tabular_files()
			.iter()
			.map(|(idx, pb)| -> (&u32, Result<File>) {
				let file: Result<File> = File::open(
					packing_list
						.locate(pb)
						.expect("couldn't locate tabular file"),
				)
				.map_err(Into::into);
				(idx, file)
			})
			.filter_map(|(idx, maybe_file)| -> Option<(u32, File)> {
				match maybe_file {
					Ok(f) => Some((*idx, f)),
					_ => None,
				}
			})
			.collect();
		let rows: usize = *packing_list.rows();

		Ok(Self {
			schema,
			header_index,
			tabular_index,
			table_locations,
			geographical_header,
			tabular_files,
			rows,
		})
	}

	pub fn from_packing_list_file<P: AsRef<Path>>(file_path: P) -> Result<Self> {
		let pl = PackingList::from_file(file_path)?;
		Self::from_packing_list(pl)
	}

	pub fn index(mut self) -> Result<Self> {
		let mut new_header_index = GeographicalHeaderIndex::new();
		let mut new_tabular_index = TabularIndex::default();

		log::debug!("Indexing tabular files...");

		for (idx, file) in &self.tabular_files {
			log::debug!("Indexing tabular file {}", idx);

			let file_reader = BufReader::new(file);
			let mut file_reader = csv::ReaderBuilder::new()
				.has_headers(false)
				.from_reader(file_reader);

			let mut index = LogicalRecordPositionIndex::new_with_size(self.rows);

			log::trace!("Reading records");

			let t0 = std::time::Instant::now();

			index.extend(
				file_reader
					.records()
					.filter_map(|record| record.ok())
					.map(|record| {
						let position = record.position().expect("couldn't find position of record");
						let byte_offset: u64 = position.byte();

						// NOTE Assumption made here: Logical Record Number = Line Number
						let logrecno: LogicalRecordNumber = position.line();

						(logrecno, byte_offset)
					}),
			);

			log::trace!(
				"Finished indexing in {}ns",
				std::time::Instant::now().duration_since(t0).as_nanos()
			);

			log::trace!("Adding logical record index to global index");

			new_tabular_index.insert(*idx, index);
		}

		log::debug!("Indexing geographical header file");

		let mut reader = BufReader::new(&self.geographical_header);
		let mut buf = String::new();
		let mut pos = 0_u64;

		loop {
			let bytes_read = reader.read_line(&mut buf)?;

			if bytes_read > 0 {
				if let Some((logrecno, geoid, pos)) = match self.schema {
					Schema::Census2010(_) => {
						let (logrecno, state_fips, county, tract, block) = (
							&buf[18..25],
							&buf[27..29],
							&buf[29..32],
							&buf[54..60],
							&buf[61..65],
						);

						match (state_fips, county, tract, block) {
							(_s, "   ", "      ", "    ") => None,
							(_s, _c, "      ", "    ") => None,
							(_s, _c, _t, "    ") => None,
							(s, c, t, b) => Some((logrecno.parse()?, [s, c, t, b].concat(), pos)),
						}
					}
					Schema::Census2020(_) => {
						let split: Vec<&str> = buf.split('|').collect();

						let logrecno = split[7];
						let geoid = split[9];

						Some((logrecno.parse()?, geoid.to_string(), pos))
					}
				} {
					assert!(!new_header_index.contains_key(&geoid));

					new_header_index.insert(geoid, (logrecno, pos));
				}
				pos += bytes_read as u64;
				buf.clear();
			} else {
				break;
			}
		}

		self.tabular_index = Some(new_tabular_index);
		self.header_index = Some(new_header_index);

		Ok(self)
	}
}
