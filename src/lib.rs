use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub mod error;

pub type LogicalRecordNumber = u64;

/// A trait containing behavior expected for datasets
pub trait Dataset<LogicalRecord> {
	/// Retrieve the logical record with number `number`
	fn get_logical_record(
		&self,
		number: LogicalRecordNumber,
		schemas: Vec<crate::Schema>,
	) -> Result<LogicalRecord>;
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

		pub use FileType::{GeographicalHeader, Tabular};
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
pub struct IndexedPackingListDataset {
	identifier: String,
	schema: Option<Schema>,
	index: Option<LogicalRecordIndex>,
	tables: HashMap<Schema, TableLocations>,
	files: HashMap<FileType, std::fs::File>,
}

pub(crate) type LogicalRecordIndex =
	HashMap<FileType, Mutex<csv_index::RandomAccessSimple<std::io::Cursor<Vec<u8>>>>>;

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

impl Dataset<csv::StringRecord> for IndexedPackingListDataset {
	fn get_logical_record(
		&self,
		logical_record_number: LogicalRecordNumber,
		requested_schemas: Vec<Schema>,
	) -> Result<csv::StringRecord> {
		let requested_schemas_set: HashSet<Schema> = requested_schemas.iter().copied().collect();
		let available_schemas: HashSet<Schema> = self
			.tables
			.keys()
			.copied()
			.collect::<HashSet<Schema>>()
			.intersection(&requested_schemas_set)
			.copied()
			.collect();

		assert_eq!(available_schemas, requested_schemas_set);

		log::debug!("Requesting {:?}", requested_schemas);

		let ranges = requested_schemas.iter().map(|schema| -> (Schema, TableLocations) {
			(*schema, self.tables.get(schema).unwrap().clone())
		}).flat_map(|(schema, locations)| -> Vec<(FileType, &std::fs::File, core::ops::Range<usize>)> {
			locations.iter().map(|location: &TableSegmentLocation| -> (usize, core::ops::Range<usize>) {
				(location.file, location.range.clone())
			}).map(|(file_number, columns): (usize, core::ops::Range<usize>)| -> (FileType, core::ops::Range<usize>) {
				(match schema {
					Schema::Census2010Pl94_171(Some(_)) => FileType::Census2010Pl94_171(census2010::pl94_171::Tabular(file_number)),
					_ => unimplemented!(),
				}, columns)
			})
			.map(|(fty, columns)| -> (FileType, &std::fs::File, core::ops::Range<usize>) {
				(fty, self.files.get(&fty).unwrap(), columns)
			}).collect()
		});

		match &self.index {
			Some(index) => {
				let mut record: Vec<String> = Vec::new();
				ranges.map(|(fty, file, cols): (FileType, &std::fs::File, core::ops::Range<usize>)| -> Vec<String> {
					let mut idx = index.get(&fty).unwrap().lock().unwrap();
					let br = BufReader::new(file);
					let mut reader = csv::Reader::from_reader(br);

					let offset = idx.get(logical_record_number - 1_u64).unwrap_or_else(|_| panic!("index is missing {}", logical_record_number - 1_u64));
					reader.seek(offset).expect("couldn't seek reader");

					let mut rec: csv::StringRecord = csv::StringRecord::new();
					reader.read_record(&mut rec).unwrap();
					let rec = rec;

					debug_assert!(rec[4].parse::<u64>().unwrap() == logical_record_number);

					cols.map(|col: usize| -> String {
						rec[col].to_string()
					}).collect()
				})
				.for_each(|mut table_part: Vec<String>| record.append(&mut table_part));

				Ok(csv::StringRecord::from(record))
			}
			None => unimplemented!(),
		}
	}
}

impl Default for IndexedPackingListDataset {
	fn default() -> Self {
		Self {
			identifier: "".to_string(),
			index: None,
			schema: None,
			tables: HashMap::new(),
			files: HashMap::new(),
		}
	}
}

impl IndexedPackingListDataset {
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

		let file =
			std::fs::File::open(&path).unwrap_or_else(|_| panic!("could not open {} for reading", &path));
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

		lazy_static::lazy_static! {
			static ref TABLE_INFORMATION_RE: regex::Regex =
				regex::Regex::new("^(?P<table>[A-Za-z0-9]+)\\|(?P<loc>[\\d: ]+)\\|$")
					.expect("couldn't parse regex");
			static ref FILE_INFORMATION_RE: regex::Regex = regex::Regex::new("^(?P<filename>(?P<stusab>[a-z]{2})(?P<ident>\\w+)(?P<year>\\d{4})\\.(?P<ds>.+))\\|(?P<date>.+)\\|(?P<size>\\d+)\\|(?P<lines>\\d+)\\|$").expect("couldn't parse regex");
		}

		#[derive(Clone, Debug, PartialEq)]
		enum Line {
			DataSegmentationInformation(TableName, TableLocationSpecifier),
			FileInformation(PathBuf, Schema, String),
		}

		let sections: Vec<Vec<Line>> = sections
			.map(|lines: &[String]| -> Vec<Line> {
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
			.filter(|lines| !lines.is_empty())
			.collect();

		let data_segmentation_lines: Vec<&Line> = sections
			.iter()
			.filter(|section| {
				section.iter().all(|line| match line {
					Line::DataSegmentationInformation(..) => true,
					_ => false,
				})
			})
			.flatten()
			.collect();

		log::debug!(
			"{} lines containing data segmentation information",
			data_segmentation_lines.len()
		);

		let file_information_lines: Vec<&Line> = sections
			.iter()
			.filter(|section| {
				section.iter().all(|line| match line {
					Line::FileInformation(..) => true,
					_ => false,
				})
			})
			.flatten()
			.collect();

		log::debug!(
			"{} lines containing file information",
			file_information_lines.len()
		);

		// First, load up the file information as we want it
		for line in file_information_lines {
			if let Line::FileInformation(file_name, schema, ident) = line {
				log::trace!("Processing file information line: {:?}", line);

				// Parse the File Type and attempt to get close to the right spot
				let file_type: FileType = match (schema, ident.as_str()) {
					(Schema::Census2010Pl94_171(None), "geo") => {
						FileType::Census2010Pl94_171(census2010::pl94_171::GeographicalHeader)
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
				let mut full_file_name = PathBuf::new();
				full_file_name.push(parent_directory);
				full_file_name.push(file_name);
				let file_name = full_file_name;

				log::trace!(" -> file_name = {:?}", file_name);

				let file = std::fs::File::open(&file_name)
					.unwrap_or_else(|_| panic!("couldn't open file {:?}", file_name));

				self.files.insert(file_type, file);
			}
		}

		// Next, set up the references for data segmentation information
		let mut current_column_numbers: HashMap<usize, usize> = HashMap::new();

		for line in data_segmentation_lines {
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

				let location_specifiers: &Vec<TableSegmentSpecifier> = table_location;

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
		assert!(self.index.is_none());

		let mut new_index = LogicalRecordIndex::new();

		log::debug!("Indexing tabular files...");

		let tabular_files: HashMap<&FileType, &std::fs::File> = self
			.files
			.iter()
			.filter(|(fty, _)| -> bool {
				match fty {
					FileType::Census2010Pl94_171(census2010::pl94_171::Tabular(_)) => true,
					_ => false,
				}
			})
			.collect();

		for (fty, file) in tabular_files {
			log::debug!("Indexing file with FileType {:?}", fty);

			let file_reader = BufReader::new(file);
			let mut file_reader = csv::Reader::from_reader(file_reader);
			let mut index_data = std::io::Cursor::new(vec![]);

			log::trace!("Creating index...");
			csv_index::RandomAccessSimple::create(&mut file_reader, &mut index_data)?;

			log::trace!("Opening index...");

			let index = csv_index::RandomAccessSimple::open(index_data)?;
			let index = Mutex::new(index);

			log::trace!("Adding index to registry...");

			new_index.insert(*fty, index);
		}

		self.index = Some(new_index);

		Ok(self)
	}
}
