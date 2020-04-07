use crate::error::Result;
use regex::Regex;
#[cfg(feature = "fx-hash")]
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[cfg(not(feature = "fx-hash"))]
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

pub mod error;

pub type LogicalRecordNumber = u64;
pub type GeoId = String;

#[cfg(feature = "fx-hash")]
pub(crate) type LogicalRecordPositionIndex = FxHashMap<LogicalRecordNumber, u64>;
#[cfg(not(feature = "fx-hash"))]
pub(crate) type LogicalRecordPositionIndex = HashMap<LogicalRecordNumber, u64>;

/// A Logical Record
pub trait LogicalRecord {
	/// Get the corresponding number
	///
	/// The Census refers to records by their "logical number."  A logical record
	/// is assumed _only_ to have this number.
	fn number(&self) -> LogicalRecordNumber;
}

/// A trait containing behavior expected for datasets
pub trait Dataset<LogicalRecord> {
	/// Retrieve the logical record with number `number`
	fn get_logical_record(&self, number: LogicalRecordNumber) -> Result<LogicalRecord>;

	/// Retrieve the logical record corresponding to GeoID `id`
	fn get_logical_record_number_for_geoid(&self, geoid: &str) -> Result<LogicalRecordNumber>;

	/// Retrieve the GeographicalHeader
	fn get_header_for_geoid(&self, geoid: &str) -> Result<Box<dyn GeographicalHeader>>;
}

pub struct FileBackedLogicalRecord {
	number: LogicalRecordNumber,
	#[cfg(feature = "fx-hash")]
	records: FxHashMap<usize, csv::StringRecord>,
	#[cfg(not(feature = "fx-hash"))]
	records: HashMap<usize, csv::StringRecord>,
}

impl LogicalRecord for FileBackedLogicalRecord {
	fn number(&self) -> LogicalRecordNumber {
		self.number
	}
}

impl FileBackedLogicalRecord {
	fn new(number: LogicalRecordNumber) -> Self {
		Self {
			number,
			#[cfg(feature = "fx-hash")]
			records: FxHashMap::default(),
			#[cfg(not(feature = "fx-hash"))]
			records: HashMap::default(),
		}
	}

	fn records(mut self, records: BTreeMap<usize, csv::StringRecord>) -> Self {
		self.records.extend(records);
		self
	}
}

/// A geographical header
pub trait GeographicalHeader {
	fn name(&self) -> &str;
	fn logrecno(&self) -> LogicalRecordNumber;
}

pub mod census2010;

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Schema {
	Census2010Pl94_171(Option<census2010::pl94_171::Table>),
}

impl<S: AsRef<str>> core::convert::From<S> for Schema {
	fn from(s: S) -> Self {
		let s: &str = s.as_ref();
		match s {
			"p1" => Schema::Census2010Pl94_171(Some(census2010::pl94_171::P1)),
			"p2" => Schema::Census2010Pl94_171(Some(census2010::pl94_171::P2)),
			"p3" => Schema::Census2010Pl94_171(Some(census2010::pl94_171::P3)),
			"p4" => Schema::Census2010Pl94_171(Some(census2010::pl94_171::P4)),
			"h1" => Schema::Census2010Pl94_171(Some(census2010::pl94_171::H1)),
			_ => unimplemented!(),
		}
	}
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub(crate) enum FileType {
	Census2010Pl94_171(census2010::pl94_171::FileType),
}

impl FileType {
	fn is_header(&self) -> bool {
		match self {
			Self::Census2010Pl94_171(census2010::pl94_171::FileType::GeographicalHeader) => true,
			_ => false,
		}
	}

	fn is_tabular(&self) -> bool {
		match self {
			Self::Census2010Pl94_171(census2010::pl94_171::FileType::Tabular(_)) => true,
			_ => false,
		}
	}

	fn tabular_index(&self) -> Option<usize> {
		match self {
			Self::Census2010Pl94_171(census2010::pl94_171::FileType::Tabular(n)) => Some(*n),
			_ => None,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::census2010::pl94_171::Table;
	use crate::Schema;

	#[test]
	fn schema_with_table_de() {
		let data = r"Census2010Pl94_171: P1";
		let schema: Schema = serde_yaml::from_str(data).unwrap();
		assert_eq!(schema, Schema::Census2010Pl94_171(Some(Table::P1)))
	}

	#[test]
	fn bare_schema_de() {
		let data = r"Census2010Pl94_171:";
		let schema: Schema = serde_yaml::from_str(data).unwrap();
		assert_eq!(schema, Schema::Census2010Pl94_171(None))
	}
}

/// A Census Dataset
///
/// Every dataset has a unique, human-identifiable identifier, which is used
/// internally for reading the data.
pub struct IndexedDataset {
	identifier: String,
	schema: Option<Schema>,
	header_index: Option<GeographicalHeaderIndex>,
	logical_record_index: Option<LogicalRecordIndex>,
	#[cfg(feature = "fx-hash")]
	tables: FxHashMap<Schema, TableLocations>,
	#[cfg(not(feature = "fx-hash"))]
	tables: HashMap<Schema, TableLocations>,
	#[cfg(feature = "fx-hash")]
	files: FxHashMap<FileType, File>,
	#[cfg(not(feature = "fx-hash"))]
	files: HashMap<FileType, File>,
}

pub(crate) type GeographicalHeaderIndex = BTreeMap<GeoId, (LogicalRecordNumber, u64)>;
#[cfg(feature = "fx-hash")]
pub(crate) type LogicalRecordIndex = FxHashMap<FileType, LogicalRecordPositionIndex>;
#[cfg(not(feature = "fx-hash"))]
pub(crate) type LogicalRecordIndex = HashMap<FileType, LogicalRecordPositionIndex>;

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct TableSegmentSpecifier {
	file: usize,
	columns: usize,
}

#[derive(Clone, Debug)]
pub struct TableSegmentLocation {
	file: usize,
	range: core::ops::Range<usize>,
}

pub type TableName = String;
pub type TableLocationSpecifier = Vec<TableSegmentSpecifier>;
pub type TableLocations = Vec<TableSegmentLocation>;

impl Dataset<FileBackedLogicalRecord> for IndexedDataset {
	/// Retrieve the logical record by number and by table
	fn get_logical_record(&self, number: LogicalRecordNumber) -> Result<FileBackedLogicalRecord> {
		log::debug!("Getting logical record {}", number);

		match &self.logical_record_index {
			Some(index) => {
				let records_from_file: BTreeMap<usize, csv::StringRecord> = self
					.files
					.iter()
					.filter(|(file_type, _)| file_type.is_tabular())
					.map(|(fty, file)| -> (usize, csv::StringRecord) {
						let corresponding_logrec_position_index = index.get(&fty).unwrap();
						let offset: &u64 = corresponding_logrec_position_index.get(&number).unwrap();

						use std::io::Seek;
						let mut reader = BufReader::new(file);
						reader
							.seek(std::io::SeekFrom::Start(*offset))
							.expect("couldn't seek to record");

						let mut reader = csv::ReaderBuilder::new()
							.has_headers(false)
							.from_reader(reader);
						let mut record = csv::StringRecord::new();
						reader
							.read_record(&mut record)
							.expect("couldn't read record");

						debug_assert!(record[4].parse::<LogicalRecordNumber>().unwrap() == number);

						(fty.tabular_index().expect("fty is tabular"), record)
					})
					.collect();

				log::debug!("Read records: {:?}", records_from_file);

				let record = FileBackedLogicalRecord::new(number).records(records_from_file);

				Ok(record)
			}

			None => unimplemented!(),
		}
	}

	fn get_logical_record_number_for_geoid(&self, geoid: &str) -> Result<u64> {
		if let Some(index) = &self.header_index {
			let result: &(LogicalRecordNumber, u64) = index.get(geoid).unwrap();

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

			let fty = match self.schema {
				Some(Schema::Census2010Pl94_171(_)) => {
					FileType::Census2010Pl94_171(census2010::pl94_171::FileType::GeographicalHeader)
				}
				None => panic!("dataset has no schema"),
			};

			let file = self.files.get(&fty).unwrap();

			let mut reader = BufReader::new(file);

			use std::io::Seek;
			reader.seek(std::io::SeekFrom::Start(line_offset))?;

			let mut line = String::new();
			reader.read_line(&mut line)?;

			match fty {
				FileType::Census2010Pl94_171(_) => Ok(Box::new(
					census2010::pl94_171::GeographicalHeader::new(line),
				)),
			}
		} else {
			unimplemented!()
		}
	}
}

impl Default for IndexedDataset {
	fn default() -> Self {
		Self {
			identifier: "".to_string(),
			logical_record_index: None,
			header_index: None,
			schema: None,
			#[cfg(feature = "fx-hash")]
			tables: FxHashMap::default(),
			#[cfg(not(feature = "fx-hash"))]
			tables: HashMap::default(),
			#[cfg(feature = "fx-hash")]
			files: FxHashMap::default(),
			#[cfg(not(feature = "fx-hash"))]
			files: HashMap::default(),
		}
	}
}

lazy_static::lazy_static! {
	static ref TABLE_INFORMATION_RE: Regex =
		Regex::new(r"^(?P<table>[A-Za-z0-9]+)\|(?P<loc>[\d: ]+)\|$")
			.expect("couldn't parse regex");

	static ref FILE_INFORMATION_RE: Regex =
		Regex::new(r"^(?P<filename>(?P<stusab>[a-z]{2})(?P<ident>\w+)(?P<year>\d{4})\.(?P<ds>.+))\|(?P<date>.+)\|(?P<size>\d+)\|(?P<lines>\d+)\|$")
			.expect("couldn't parse regex");
}

impl IndexedDataset {
	pub fn new<S: Into<String>>(s: S) -> Self {
		Self {
			identifier: s.into(),
			..Self::default()
		}
	}

	/// Load the dataset according to the packing list
	///
	/// # Panics
	///
	/// - Panics if the cache of tables is not empty
	/// - Panics if the cache of files is not empty
	/// - Panics if the specified file could not be opened for reading
	/// - Panics if at any point while reading a line cannot be parsed to a String (i.e. contains invalid Utf8)
	/// - Panics if the packing list contains Data Segmentation Information containing a table location specifier that cannot be split on `:` into at least two groups
	/// - Panics if the packing list contains Data Segmentation Information containing a table location specifier whose file number cannot be parsed as `usize`
	/// - Panics if the packing list contains Data Segmentation Information containing a table location specifier whose width cannot be parsed as `usize`
	/// - Panics if the packing list contains File Information whose year component and extension are not recognized
	/// - Panics if a line matches the Data Segmentation Information regular expression but does not have one of the required capture groups
	/// - Panics if a line matches the File Information regular expression but does not have one of the required capture groups
	pub fn unpack<P: core::fmt::Display + AsRef<Path>>(mut self, path: P) -> Result<Self> {
		assert!(self.tables.is_empty());
		assert!(self.files.is_empty());

		log::debug!("Opening {} for reading", &path);

		let file = File::open(&path).unwrap_or_else(|_| panic!("could not open {} for reading", &path));
		let stream = BufReader::new(file);

		log::debug!("Successfully opened {}", &path);

		// Stream -> Data

		#[derive(Clone, Debug, PartialEq)]
		enum Line {
			DataSegmentationInformation(TableName, TableLocationSpecifier),
			FileInformation(PathBuf, Schema, String),
		}

		impl core::convert::TryFrom<regex::Captures<'_>> for Line {
			type Error = crate::error::Error;

			fn try_from(captures: regex::Captures) -> crate::error::Result<Self> {
				match (
					captures.name("table"),
					captures.name("loc"),
					captures.name("filename"),
					captures.name("ident"),
					captures.name("year"),
					captures.name("ds"),
				) {
					(Some(table_name), Some(table_locations), None, None, None, None) => {
						let table_name = table_name.as_str().to_string();

						let table_locations: Vec<TableSegmentSpecifier> = table_locations
							.as_str()
							.split(' ')
							.map(|chunk| -> TableSegmentSpecifier {
								let split: Vec<&str> = chunk.split(':').collect();
								log::trace!("{:?}, {:?}", captures, split);
								let file = split[0].parse().expect("couldn't parse file idx");
								let columns = split[1].parse().expect("couldn't parse width");
								TableSegmentSpecifier { file, columns }
							})
							.collect();

						Ok(Line::DataSegmentationInformation(
							table_name,
							table_locations,
						))
					}

					(None, None, Some(filename), Some(ident), Some(year), Some(ds)) => {
						let filename: PathBuf = filename.as_str().into();

						let schema: Schema = match (year.as_str(), ds.as_str()) {
							("2010", "pl") => Schema::Census2010Pl94_171(None),
							_ => unimplemented!(),
						};

						Ok(Line::FileInformation(
							filename,
							schema,
							ident.as_str().to_string(),
						))
					}

					(_, _, _, _, _, _) => panic!("unexpected capture grouping"),
				}
			}
		}

		log::debug!("Parsing packing list information from {}", &path);

		let lines = stream
			.lines()
			.map(|maybe_line| maybe_line.expect("couldn't read line"));

		let lines: Vec<Line> = lines
			.flat_map(|line: String| -> Option<Line> {
				use core::convert::TryInto;

				if let Some(captures) = TABLE_INFORMATION_RE.captures(&line) {
					Some(
						captures
							.try_into()
							.expect("couldn't convert data segmentation information captures to line"),
					)
				} else if let Some(captures) = FILE_INFORMATION_RE.captures(&line) {
					Some(
						captures
							.try_into()
							.expect("couldn't convert file information captures to line"),
					)
				} else {
					None
				}
			})
			.collect();

		// First, load up the file information as we want it
		for line in &lines {
			if let Line::FileInformation(file_name, schema, ident) = line {
				log::trace!("Processing file information line: {:?}", line);

				// Parse the File Type and attempt to get close to the right spot
				let file_type: FileType = match (schema, ident.as_str()) {
					(Schema::Census2010Pl94_171(None), "geo") => {
						FileType::Census2010Pl94_171(census2010::pl94_171::FileType::GeographicalHeader)
					}
					(Schema::Census2010Pl94_171(None), maybe_numeric) => FileType::Census2010Pl94_171(
						census2010::pl94_171::Tabular(maybe_numeric.parse::<usize>().unwrap()),
					),
					_ => unimplemented!(),
				};

				log::trace!(" -> file_type = {:?}", file_type);

				if self.schema.is_none() {
					let dataset_schema = match file_type {
						FileType::Census2010Pl94_171(_) => Schema::Census2010Pl94_171(None),
					};

					log::debug!("Inferred dataset schema of {:?}", dataset_schema);

					self.schema = Some(dataset_schema);
				}

				let parent_directory = path
					.as_ref()
					.parent()
					.expect("packing list path must be a file");

				let file_name = {
					let mut path = PathBuf::new();
					path.push(parent_directory);
					path.push(file_name);
					path
				};

				log::trace!(" -> file_name = {:?}", file_name);

				let file =
					File::open(&file_name).unwrap_or_else(|_| panic!("couldn't open file {:?}", file_name));

				self.files.insert(file_type, file);
			}
		}

		// Next, set up the references for data segmentation information
		#[cfg(feature = "fx-hash")]
		let mut current_column_numbers: FxHashMap<usize, usize> = FxHashMap::default();
		#[cfg(not(feature = "fx-hash"))]
		let mut current_column_numbers: HashMap<usize, usize> = HashMap::default();

		for line in &lines {
			if let Line::DataSegmentationInformation(table_name, table_location) = line {
				log::trace!("Processing Data Segmentation line: {:?}", line);

				let schema = match (self.schema, table_name.as_str()) {
					(Some(Schema::Census2010Pl94_171(None)), "p1") => {
						Schema::Census2010Pl94_171(Some(census2010::pl94_171::P1))
					}
					(Some(Schema::Census2010Pl94_171(None)), "p2") => {
						Schema::Census2010Pl94_171(Some(census2010::pl94_171::P2))
					}
					(Some(Schema::Census2010Pl94_171(None)), "p3") => {
						Schema::Census2010Pl94_171(Some(census2010::pl94_171::P3))
					}
					(Some(Schema::Census2010Pl94_171(None)), "p4") => {
						Schema::Census2010Pl94_171(Some(census2010::pl94_171::P4))
					}
					(Some(Schema::Census2010Pl94_171(None)), "h1") => {
						Schema::Census2010Pl94_171(Some(census2010::pl94_171::H1))
					}
					(Some(Schema::Census2010Pl94_171(Some(_))), _) => {
						panic!("schema contains table information")
					}
					(Some(Schema::Census2010Pl94_171(None)), table) => panic!("unrecognized table {}", table),
					(None, _) => panic!("schema unknown"),
				};

				let location_specifiers: &Vec<TableSegmentSpecifier> = &table_location;

				let mut locations: Vec<TableSegmentLocation> = Vec::new();

				for table_segment_spec in location_specifiers {
					let file_number: usize = table_segment_spec.file;

					// If our file doesn't already have a corresponding column counter, start at 5
					current_column_numbers.entry(file_number).or_insert(5_usize);

					let current_column_number: usize = {
						*current_column_numbers
							.get(&file_number)
							.expect("should have a column for current file")
					};

					log::debug!(
						"Current column number for {:?}: {:?}",
						schema,
						current_column_number
					);

					let width: usize = table_segment_spec.columns;

					let new_column_number = current_column_number + width;

					log::debug!(
						"New column number for {:?}: {:?}",
						schema,
						new_column_number
					);

					current_column_numbers.insert(file_number, new_column_number);

					let range: core::ops::Range<usize> = current_column_number..new_column_number;

					let location = TableSegmentLocation {
						file: file_number,
						range,
					};

					log::debug!("Added column location: {:?}", location);

					locations.push(location);
				}

				self.tables.insert(schema, locations);
			}
		}

		Ok(self)
	}

	pub fn index(mut self) -> Result<Self> {
		assert!(self.logical_record_index.is_none());

		let mut new_header_index = GeographicalHeaderIndex::new();
		let mut new_logical_record_index = LogicalRecordIndex::default();

		log::debug!("Indexing tabular files...");

		for (fty, file) in &self.files {
			match fty {
				FileType::Census2010Pl94_171(census2010::pl94_171::Tabular(tabular_file_number)) => {
					log::debug!("Indexing tabular file {}", tabular_file_number);

					let file_reader = BufReader::new(file);
					let mut file_reader = csv::ReaderBuilder::new()
						.has_headers(false)
						.from_reader(file_reader);
					#[cfg(feature = "fx-hash")]
					let mut index = FxHashMap::default();
					#[cfg(not(feature = "fx-hash"))]
					let mut index = HashMap::default();

					log::trace!("Creating index...");

					for record in file_reader.records() {
						let record: csv::StringRecord = record?;
						let position = record.position().expect("couldn't find position of record");

						let byte_offset: u64 = position.byte();
						let logrecno: LogicalRecordNumber = record[4]
							.parse::<LogicalRecordNumber>()
							.expect("couldn't parse logical record number");

						index.insert(logrecno, byte_offset);
					}

					log::trace!("Adding index to registry...");

					new_logical_record_index.insert(*fty, index);
				}

				FileType::Census2010Pl94_171(census2010::pl94_171::FileType::GeographicalHeader) => {
					log::debug!("Indexing geographical header file");

					let mut reader = BufReader::new(file);
					let mut buf = String::new();
					let mut pos = 0_u64;

					loop {
						let bytes = reader.read_line(&mut buf)?;

						if bytes > 0 {
							let logrecno = &buf[18..25];
							let state_fips = &buf[27..29];
							let county = &buf[29..32];
							let tract = &buf[54..60];
							let block = &buf[61..65];

							match (state_fips, county, tract, block) {
								(_s, "   ", "      ", "    ") => {}
								(_s, _c, "      ", "    ") => {}
								(_s, _c, _t, "    ") => {}
								(s, c, t, b) => {
									let logrecno: LogicalRecordNumber = logrecno.parse()?;
									let geoid: GeoId = [s, c, t, b].concat();

									assert!(!new_header_index.contains_key(&geoid));

									new_header_index.insert(geoid, (logrecno, pos));
								}
							};

							pos += bytes as u64;
							buf.clear();
						} else {
							break;
						}
					}
				}
			}
		}

		self.logical_record_index = Some(new_logical_record_index);
		self.header_index = Some(new_header_index);

		Ok(self)
	}
}
