#![allow(dead_code)]
#![allow(unused_variables)]

use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::path::PathBuf;

pub mod error;

pub type LogicalRecordNumber = u32;

/// A trait containing behavior expected for datasets
pub trait Dataset<LogicalRecord> {
	/// Retrieve the logical record with number `number`
	fn get_logical_record(&self, number: LogicalRecordNumber) -> crate::error::Result<LogicalRecord>;
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

		#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
		pub enum FileType {
			Tabular(usize),
			GeographicalHeader
		}
	}
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub(crate) enum Schema {
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
	tables: std::collections::HashMap<Schema, TableLocations>,
	files: std::collections::HashMap<FileType, std::fs::File>,
}

pub struct LogicalRecordIndex {}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct TableSegmentSpecifier {
	file: usize,
	columns: usize,
}

#[derive(Debug)]
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
	) -> crate::error::Result<csv::StringRecord> {
		match &self.index {
			Some(idx) => unimplemented!(),
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
			tables: std::collections::HashMap::new(),
			files: std::collections::HashMap::new(),
		}
	}
}

impl IndexedPackingListDataset {
	pub fn new<S: Into<String>>(s: S) -> Self {
		Self {
			identifier: s.into(),
			..Default::default()
		}
	}

	pub fn packing_list<P: core::fmt::Display + AsRef<Path>>(mut self, path: P) -> Self {
		assert!(self.tables.is_empty());
		assert!(self.files.is_empty());

		log::debug!("Opening {} for reading", &path);

		let file = std::fs::File::open(&path).expect(&format!("could not open {} for reading", &path));
		let stream = BufReader::new(file);

		log::debug!("Successfully opened {}", &path);

		// Stream -> Sections

		log::debug!("Reading lines from {}", &path);

		let delimiter: String = "#".repeat(80);
		let lines: Vec<String> = stream
			.lines()
			.map(|r| r.expect("couldn't parse line"))
			.collect();

		log::debug!("Splitting lineset into sections");

		let sections = lines
			.split(|line| line == &"#".repeat(80) || line == &"#".repeat(81))
			.filter(|section| section.len() > 0 && !(section.iter().all(|line| line.trim().len() == 0)));

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
								.split(" ")
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
			.filter(|lines| lines.len() > 0)
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

		log::debug!("{} lines containing data segmentation information", data_segmentation_lines.len());

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

		log::debug!("{} lines containing file information", file_information_lines.len());

		// First, load up the file information as we want it
		for line in file_information_lines {
			if let Line::FileInformation(file_name, schema, ident) = line {
				log::trace!("Processing file information line: {:?}", line);

				// Parse the File Type and attempt to get close to the right spot
				let file_type: FileType = match (schema, ident.as_str()) {
					(Schema::Census2010Pl94_171(None), "geo") => FileType::Census2010Pl94_171(census2010::pl94_171::FileType::GeographicalHeader),
					(Schema::Census2010Pl94_171(None), maybe_numeric) => FileType::Census2010Pl94_171(census2010::pl94_171::FileType::Tabular(maybe_numeric.parse::<usize>().unwrap())),
					_ => unimplemented!(),
				};

				log::trace!(" -> file_type = {:?}", file_type);

				if self.schema.is_none() {
					let dataset_schema = match file_type {
						FileType::Census2010Pl94_171(_) => Schema::Census2010Pl94_171(None),
						_ => unimplemented!(),
					};

					log::debug!("Inferred dataset schema of {:?}", dataset_schema);

					self.schema = Some(dataset_schema);
				}

				let parent_directory = path.as_ref().parent().expect("packing list path must be a file");
				let mut full_file_name = PathBuf::new();
				full_file_name.push(parent_directory);
				full_file_name.push(file_name);
				let file_name = full_file_name;

				log::trace!(" -> file_name = {:?}", file_name);

				let file = std::fs::File::open(&file_name).expect(&format!("couldn't open file {:?}", file_name));

				self.files.insert(file_type, file);
			}
		}

		// Next, set up the references for data segmentation information
		let mut current_column_numbers: std::collections::HashMap<usize, usize> = std::collections::HashMap::new();

		for line in data_segmentation_lines {
			if let Line::DataSegmentationInformation(table_name, table_location) = line {
				log::trace!("Processing Data Segmentation line: {:?}", line);

				let schema = match (self.schema, table_name.as_str()) {
					(Some(Schema::Census2010Pl94_171(None)), "p1") => Schema::Census2010Pl94_171(Some(census2010::pl94_171::Table::P1)),
					(Some(Schema::Census2010Pl94_171(None)), "p2") => Schema::Census2010Pl94_171(Some(census2010::pl94_171::Table::P2)),
					(Some(Schema::Census2010Pl94_171(None)), "p3") => Schema::Census2010Pl94_171(Some(census2010::pl94_171::Table::P3)),
					(Some(Schema::Census2010Pl94_171(None)), "p4") => Schema::Census2010Pl94_171(Some(census2010::pl94_171::Table::P4)),
					(Some(Schema::Census2010Pl94_171(None)), "h1") => Schema::Census2010Pl94_171(Some(census2010::pl94_171::Table::H1)),
					(Some(Schema::Census2010Pl94_171(Some(_))), _) => panic!("schema contains table information"),
					(Some(Schema::Census2010Pl94_171(None)), table) => panic!("unrecognized table {}", table),
					(None, _) => panic!("schema unknown"),
				};

				let location_specifiers: &Vec<TableSegmentSpecifier> = table_location;

				let mut locations: Vec<TableSegmentLocation> = Vec::new();

				for table_segment_spec in location_specifiers {
					let file_number: usize = table_segment_spec.file;

					if !current_column_numbers.contains_key(&file_number) {
						current_column_numbers.insert(file_number, 5_usize);
					}

					let current_column_number: usize = {
						*current_column_numbers.get(&file_number).expect("should have a column for current file")
					};

					log::debug!("Current column number for {:?}: {:?}", schema, current_column_number);

					let width: usize = table_segment_spec.columns;

					let new_column_number = current_column_number + width;

					log::debug!("New column number for {:?}: {:?}", schema, new_column_number);

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

		self
	}

	pub fn index(&self) -> crate::error::Result<()> {
		unimplemented!()
	}
}
