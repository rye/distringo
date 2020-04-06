use crate::error::Result;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

pub mod error;

pub type LogicalRecordNumber = u64;
pub type GeoId = String;

pub(crate) type LogicalRecordPositionIndex = HashMap<LogicalRecordNumber, u64>;

/// A trait containing behavior expected for datasets
pub trait Dataset<LogicalRecord> {
	/// Retrieve the logical record with number `number`
	fn get_logical_record(
		&self,
		number: LogicalRecordNumber,
		schemas: Vec<crate::Schema>,
	) -> Result<LogicalRecord>;

	/// Retrieve the logical record corresponding to GeoID `id`
	fn get_logical_record_number_for_geoid(&self, geoid: &str) -> Result<LogicalRecordNumber>;

	fn get_header_for_geoid(&self, geoid: &str) -> Result<Box<dyn GeographicalHeader>>;
}

/// A geographical header
pub trait GeographicalHeader {
	fn name(&self) -> &str;
}

pub mod census2010 {
	pub mod pl94_171 {
		use serde::{Deserialize, Serialize};

		#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
		pub enum Table {
			P1,
			P2,
			P3,
			P4,
			H1,
		}

		pub use Table::{H1, P1, P2, P3, P4};

		#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
		pub enum FileType {
			Tabular(usize),
			GeographicalHeader,
		}

		pub use FileType::Tabular;

		macro_rules! generate_field_getter {
			($container_type:ty, $container_data_field:ident, $name:ident, [$vis:vis $getter_name:ident -> $pty:ty]) => {
				#[allow(dead_code)]
				impl $container_type {
					#[must_use]
					$vis fn $getter_name(&self) -> $pty {
						self.$container_data_field[$name].parse::<$pty>().unwrap()
					}
				}
			};
			($container_type:ty, $container_data_field:ident, $name:ident, [$vis:vis $getter_name:ident]) => {
				#[allow(dead_code)]
				impl $container_type {
					#[must_use]
					$vis fn $getter_name(&self) -> &str {
						&self.$container_data_field[$name]
					}
				}
			};
		}

		macro_rules! field {
			($container_type:ident, $container_data_field:ident, $($name:ident @ { $loc:expr } - $rest:tt),+) => {
				$(
					#[allow(dead_code)]
					const $name: core::ops::Range<usize> =  $loc;

					generate_field_getter!($container_type, $container_data_field, $name, $rest);
				)+
			};
		}

		field!(
			GeographicalHeader, data,
			FILEID @ { 0..6 } - [pub fileid],
			STUSAB @ { 6..8 } - [pub stusab],
			SUMLEV @ { 8..11 } - [pub sumlev],
			GEOCOMP @ { 11..13 } - [pub geocomp],
			CHARITER @ { 13..16 } - [pub chariter],
			CIFSN @ { 16..18 } - [pub cifsn],
			LOGRECNO @ { 18..25 } - [pub logrecno -> crate::LogicalRecordNumber],
			REGION @ { 25..26 } - [pub region],
			DIVISION @ { 26..27 } - [pub division],
			STATE @ { 27..29 } - [pub state],
			COUNTY @ { 29..32 } - [pub county],
			COUNTYCC @ { 32..34 } - [pub countycc],
			COUNTYSC @ { 34..36 } - [pub countysc],
			COUSUB @ { 36..41 } - [pub cousub],
			COUSUBCC @ { 41..43 } - [pub cousubcc],
			COUSUBSC @ { 43..45 } - [pub cousubsc],
			PLACE @ { 45..50 } - [pub place],
			PLACECC @ { 50..52 } - [pub placecc],
			PLACESC @ { 52..54 } - [pub placesc],
			TRACT @ { 54..60 } - [pub tract],
			BLKGRP @ { 60..61 } - [pub blkgrp],
			BLOCK @ { 61..65 } - [pub block],
			IUC @ { 65..67 } - [pub iuc],
			CONCIT @ { 67..72 } - [pub concit],
			CONCITCC @ { 72..74 } - [pub concitcc],
			CONCITSC @ { 74..76 } - [pub concitsc],
			AIANHH @ { 76..80 } - [pub aianhh],
			AIANHHFP @ { 80..85 } - [pub aianhhfp],
			AIANHHCC @ { 85..87 } - [pub aianhhcc],
			AIHHTLI @ { 87..88 } - [pub aihhtli],
			AITSCE @ { 88..91 } - [pub aitsce],
			AITS @ { 91..96 } - [pub aits],
			AITSCC @ { 96..98 } - [pub aitscc],
			TTRACT @ { 98..104 } - [pub ttract],
			TBLKGRP @ { 104..105 } - [pub tblkgrp],
			ANRC @ { 105..110 } - [pub anrc],
			ANRCCC @ { 110..112 } - [pub anrccc],
			CBSA @ { 112..117 } - [pub cbsa],
			CBASC @ { 117..119 } - [pub cbasc],
			METDIV @ { 119..124 } - [pub metdiv],
			CSA @ { 124..127 } - [pub csa],
			NECTA @ { 127..132 } - [pub necta],
			NECTASC @ { 132..134 } - [pub nectasc],
			NECTADIV @ { 134..139 } - [pub nectadiv],
			CNECTA @ { 139..142 } - [pub cnecta],
			CBSAPCI @ { 142..143 } - [pub cbsapci],
			NECTAPCI @ { 143..144 } - [pub nectapci],
			UA @ { 144..149 } - [pub ua],
			UASC @ { 149..151 } - [pub uasc],
			UATYPE @ { 151..152 } - [pub uatype],
			UR @ { 152..153 } - [pub ur],
			CD @ { 153..155 } - [pub cd],
			SLDU @ { 155..158 } - [pub sldu],
			SLDL @ { 158..161 } - [pub sldl],
			VTD @ { 161..167 } - [pub vtd],
			VTDI @ { 167..168 } - [pub vtdi],
			RESERVE2 @ { 168..171 } - [reserve2],
			ZCTA5 @ { 171..176 } - [pub zcta5],
			SUBMCD @ { 176..181 } - [pub submcd],
			SUBMCDCC @ { 181..183 } - [pub submcdcc],
			SDELM @ { 183..188 } - [pub sdelm],
			SDSEC @ { 188..193 } - [pub sdsec],
			SDUNI @ { 193..198 } - [pub sduni],
			AREALAND @ { 198..212 } - [pub arealand],
			AREAWATR @ { 212..226 } - [pub areawatr],
			NAME @ { 226..316 } - [pub name],
			FUNCSTAT @ { 316..317 } - [pub funcstat],
			GCUNI @ { 317..318 } - [pub gcuni],
			POP100 @ { 318..327 } - [pub pop100],
			HU100 @ { 327..336 } - [pub hu100],
			INTPTLAT @ { 336..347 } - [pub intptlat],
			INTPTLON @ { 347..359 } - [pub intptlon],
			LSADC @ { 359..361 } - [pub lsadc],
			PARTFLAG @ { 361..362 } - [pub partflag],
			RESERVE3 @ { 362..368 } - [reserve3],
			UGA @ { 368..373 } - [pub uga],
			STATENS @ { 373..381 } - [pub statens],
			COUNTYNS @ { 381..389 } - [pub countyns],
			COUSUBNS @ { 389..397 } - [pub cousubns],
			PLACENS @ { 397..405 } - [pub placens],
			CONCITNS @ { 405..413 } - [pub concitns],
			AIANHHNS @ { 413..421 } - [pub aianhhns],
			AITSNS @ { 421..429 } - [pub aitsns],
			ANRCNS @ { 429..437 } - [pub anrcns],
			SUBMCDNS @ { 437..445 } - [pub submcdns],
			CD113 @ { 445..447 } - [pub cd113],
			CD114 @ { 447..449 } - [pub cd114],
			CD115 @ { 449..451 } - [pub cd115],
			SLDU2 @ { 451..454 } - [pub sldu2],
			SLDU3 @ { 454..457 } - [pub sldu3],
			SLDU4 @ { 457..460 } - [pub sldu4],
			SLDL2 @ { 460..463 } - [pub sldl2],
			SLDL3 @ { 463..466 } - [pub sldl3],
			SLDL4 @ { 466..469 } - [pub sldl4],
			AIANHHSC @ { 469..471 } - [pub aianhhsc],
			CSASC @ { 471..473 } - [pub csasc],
			CNECTASC @ { 473..475 } - [pub cnectasc],
			MEMI @ { 475..476 } - [pub memi],
			NMEMI @ { 476..477 } - [pub nmemi],
			PUMA @ { 477..482 } - [pub puma],
			RESERVED @ { 482..500 } - [reserved]
		);

		pub struct GeographicalHeader {
			data: String,
		}

		impl GeographicalHeader {
			pub fn new(data: String) -> Self {
				Self { data }
			}
		}

		impl crate::GeographicalHeader for GeographicalHeader {
			fn name(&self) -> &str {
				&self.name()
			}
		}
	}
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Schema {
	Census2010Pl94_171(Option<census2010::pl94_171::Table>),
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub(crate) enum FileType {
	Census2010Pl94_171(census2010::pl94_171::FileType),
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
	tables: HashMap<Schema, TableLocations>,
	files: HashMap<FileType, File>,
}

pub(crate) type GeographicalHeaderIndex = BTreeMap<GeoId, (LogicalRecordNumber, u64)>;
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

impl Dataset<csv::StringRecord> for IndexedDataset {
	fn get_logical_record(
		&self,
		logical_record_number: LogicalRecordNumber,
		requested_schemas: Vec<Schema>,
	) -> Result<csv::StringRecord> {
		log::debug!("Requesting {:?}", requested_schemas);

		let ranges: Vec<(FileType, &File, core::ops::Range<usize>)> = requested_schemas.iter().map(|schema| -> (Schema, TableLocations) {
			(*schema, self.tables.get(schema).unwrap().clone())
		}).flat_map(|(schema, locations)| -> Vec<(FileType, &File, core::ops::Range<usize>)> {
			locations.iter().map(|location: &TableSegmentLocation| -> (usize, core::ops::Range<usize>) {
				(location.file, location.range.clone())
			}).map(|(file_number, columns): (usize, core::ops::Range<usize>)| -> (FileType, core::ops::Range<usize>) {
				(match schema {
					Schema::Census2010Pl94_171(Some(_)) => FileType::Census2010Pl94_171(census2010::pl94_171::Tabular(file_number)),
					_ => unimplemented!(),
				}, columns)
			})
			.map(|(fty, columns)| -> (FileType, &File, core::ops::Range<usize>) {
				(fty, self.files.get(&fty).unwrap(), columns)
			}).collect()
		})
		.collect();

		match &self.logical_record_index {
			Some(index) => {
				let ftys: std::collections::HashSet<FileType> = ranges
					.iter()
					.map(|(fty, _, _)| -> FileType { *fty })
					.collect();

				// TODO leave as iter
				let records_from_file: HashMap<FileType, csv::StringRecord> = ftys
					.iter()
					.map(|fty| -> (FileType, csv::StringRecord) {
						let file: &File = self.files.get(fty).unwrap();

						let corresponding_logrec_position_index = index.get(&fty).unwrap();
						let offset: &u64 = corresponding_logrec_position_index
							.get(&logical_record_number)
							.unwrap();

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

						debug_assert!(
							record[4].parse::<LogicalRecordNumber>().unwrap() == logical_record_number
						);

						(*fty, record)
					})
					.collect();

				log::debug!("Read records: {:?}", records_from_file);

				let mut record: Vec<String> = Vec::new();

				ranges
					.iter()
					.map(|(fty, _, cols)| -> Vec<String> {
						let record: &csv::StringRecord = records_from_file.get(&fty).unwrap();
						let cols: core::ops::Range<usize> = cols.clone();
						cols
							.map(|col: usize| -> String { record[col].to_string() })
							.collect()
					})
					.for_each(|mut table_chunk| record.append(&mut table_chunk));

				Ok(csv::StringRecord::from(record))
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
			tables: HashMap::new(),
			files: HashMap::new(),
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

	pub fn unpack<P: core::fmt::Display + AsRef<Path>>(mut self, path: P) -> Result<Self> {
		assert!(self.tables.is_empty());
		assert!(self.files.is_empty());

		log::debug!("Opening {} for reading", &path);

		let file = File::open(&path).unwrap_or_else(|_| panic!("could not open {} for reading", &path));
		let stream = BufReader::new(file);

		log::debug!("Successfully opened {}", &path);

		// Stream -> Sections

		log::debug!("Reading lines from {}", &path);

		let lines: Vec<String> = stream
			.lines()
			.map(|r| r.expect("couldn't parse line"))
			.collect();

		log::debug!("Splitting lineset into sections");

		let sections = lines
			.split(|line| line == &"#".repeat(80) || line == &"#".repeat(81))
			.filter(|section| {
				!section.is_empty() && !(section.iter().all(|line| line.trim().is_empty()))
			});

		// Sections -> Data

		log::debug!("Parsing packing list information");

		#[derive(Clone, Debug, PartialEq)]
		enum Line {
			DataSegmentationInformation(TableName, TableLocationSpecifier),
			FileInformation(PathBuf, Schema, String),
		}

		let lines: Vec<Line> = sections
			.flat_map(|lines: &[String]| -> Vec<Line> {
				lines
					.iter()
					.filter_map(|line: &String| -> Option<Line> {
						if let Some(captures) = TABLE_INFORMATION_RE.captures(line) {
							let table_name = captures
								.name("table")
								.expect("missing capture group for table name");
							let table_name = table_name.as_str().to_string();

							let table_locations = captures
								.name("loc")
								.expect("missing capture group for table locations");

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
							Some(Line::DataSegmentationInformation(
								table_name,
								table_locations,
							))
						} else if let Some(captures) = FILE_INFORMATION_RE.captures(line) {
							let filename = captures
								.name("filename")
								.expect("missing capture group for file name");
							let ident = captures
								.name("ident")
								.expect("missing capture group for identifier");
							let year = captures
								.name("year")
								.expect("missing capture group for year");
							let ds = captures
								.name("ds")
								.expect("missing capture group for file extension (dataset)");

							let filename: PathBuf = filename.as_str().into();

							let schema: Schema = match (year.as_str(), ds.as_str()) {
								("2010", "pl") => Schema::Census2010Pl94_171(None),
								_ => unimplemented!(),
							};

							Some(Line::FileInformation(
								filename,
								schema,
								ident.as_str().to_string(),
							))
						} else {
							None
						}
					})
					.collect()
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
		let mut current_column_numbers: HashMap<usize, usize> = HashMap::new();

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
		let mut new_logical_record_index = LogicalRecordIndex::new();

		log::debug!("Indexing tabular files...");

		for (fty, file) in &self.files {
			match fty {
				FileType::Census2010Pl94_171(census2010::pl94_171::Tabular(tabular_file_number)) => {
					log::debug!("Indexing tabular file {}", tabular_file_number);

					let file_reader = BufReader::new(file);
					let mut file_reader = csv::ReaderBuilder::new()
						.has_headers(false)
						.from_reader(file_reader);
					let mut index = HashMap::new();

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
