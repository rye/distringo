use core::convert::TryInto;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::path::PathBuf;

use super::Dataset;

use crate::census2010;
use crate::error::Result;
use crate::FileBackedLogicalRecord;
use crate::FileType;
use crate::GeoId;
use crate::GeographicalHeader;
use crate::GeographicalHeaderIndex;
use crate::LogicalRecordIndex;
use crate::LogicalRecordNumber;
use crate::LogicalRecordPositionIndex;
use crate::OldSchema;
use crate::TableLocationSpecifier;
use crate::TableLocations;
use crate::TableName;
use crate::TableSegmentLocation;
use crate::TableSegmentSpecifier;

use fnv::FnvHashMap;
use regex::Regex;

/// A Census Dataset
///
/// Every dataset has a unique, human-identifiable identifier, which is used
/// internally for reading the data.
pub struct IndexedDataset {
	identifier: String,
	schema: Option<OldSchema>,
	header_index: Option<GeographicalHeaderIndex>,
	logical_record_index: Option<LogicalRecordIndex>,
	tables: FnvHashMap<OldSchema, TableLocations>,
	files: FnvHashMap<FileType, File>,
}

pub struct PackingList {
	schema: Schema,
	directory: Option<PathBuf>,
	table_locations: FnvHashMap<Table, TableLocations>,
	tabular_files: FnvHashMap<u32, PathBuf>,
	geographical_header_file: PathBuf,
}

fn read_file_to_string<P: AsRef<Path>>(path: P) -> Result<String> {
	use std::io::Read;

	let mut file = File::open(&path)?;
	let mut data = String::new();
	file.read_to_string(&mut data)?;

	Ok(data)
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum Schema {
	Census2010(census2010::Schema),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum Table {
	Census2010(census2010::Table),
}

impl core::str::FromStr for PackingList {
	type Err = crate::error::Error;
	fn from_str(s: &str) -> Result<Self> {
		// PHASE 0: Build the regex for the stusab

		let stusab: String = {
			let stusab_all_caps: &str = &STUSAB_RE.captures(s).expect("failed to find STUSAB")["stusab"];
			let stusab: String = stusab_all_caps.to_lowercase();
			stusab
		};

		// TODO rather than building this regex at parse time, maybe store a lazy_static cache of these for each stusab somewhere?
		let filename_re: Regex = Regex::new(&format!(
			r"(?m){}(?P<inner>\w*)(?P<year>\d{{4}})\.(?P<ext>[a-z1-9\-]*)\b",
			stusab
		))
		.expect("failed to create generated regex");

		// PHASE 1: Collect the Schema

		let mut schemas: Vec<Schema> = filename_re
			.captures_iter(s)
			.map(|captures| {
				let captures: (Option<&str>, Option<&str>, Option<&str>) = (
					captures.name("inner").as_ref().map(regex::Match::as_str),
					captures.name("year").as_ref().map(regex::Match::as_str),
					captures.name("ext").as_ref().map(regex::Match::as_str),
				);

				match captures {
					(Some(_), Some("2010"), Some("pl")) => Schema::Census2010(census2010::Schema::Pl94_171),
					_ => unimplemented!(),
				}
			})
			.collect();

		schemas.dedup();

		assert_eq!(schemas.len(), 1);

		let schema: Schema = schemas.remove(0);

		// PHASE 2: Collect the file information

		#[derive(Debug, PartialEq)]
		enum FileType {
			Tabular(u32),
			GeographicalHeader,
		}

		struct DataSegmentation {
			table_name: String,
			table_locations: Vec<TableLocations>,
		}

		#[derive(Debug)]
		struct FileInformation {
			filename: PathBuf,
			date: String,
			file_size: usize,
			rows: usize,
			ty: FileType,
		}

		let (tabular_files, geographical_header_file): (FnvHashMap<u32, PathBuf>, PathBuf) = {
			let file_informations: Vec<FileInformation> = FILE_INFORMATION_RE_ML
				.captures_iter(s)
				.map(|captures| {
					let (filename, date, size, rows): (&str, &str, &str, &str) = (
						captures
							.name("filename")
							.expect("missing capture group filename")
							.as_str(),
						captures
							.name("date")
							.expect("missing capture group date")
							.as_str(),
						captures
							.name("size")
							.expect("missing capture group size")
							.as_str(),
						captures
							.name("lines")
							.expect("missing capture group lines")
							.as_str(),
					);
					let filename = filename.to_string().into();
					let date = date.to_string();
					let file_size: usize = size.parse().expect("couldn't parse size as usize");
					let rows: usize = rows.parse().expect("couldn't parse rows as usize");
					let ty: FileType = match captures
						.name("ident")
						.expect("missing capture group ident")
						.as_str()
					{
						"geo" => FileType::GeographicalHeader,
						n => {
							if let Some(idx) = n.parse::<u32>().ok() {
								FileType::Tabular(idx)
							} else {
								unimplemented!()
							}
						}
					};
					FileInformation {
						filename,
						date,
						file_size,
						rows,
						ty,
					}
				})
				.collect();

			let header: PathBuf = file_informations
				.iter()
				.find(|fi| fi.ty == FileType::GeographicalHeader)
				.map(|fi| fi.filename.clone())
				.expect("missing geographical header");
			let tabular_files: FnvHashMap<u32, PathBuf> = file_informations
				.iter()
				.filter_map(|fi| match fi.ty {
					FileType::Tabular(idx) => Some((idx, fi.filename.clone())),
					_ => None,
				})
				.collect();

			(tabular_files, header)
		};

		// PHASE 3: Calculate the table locations

		// TODO consider just hard-coding the table locations in our spec

		let table_locations: FnvHashMap<Table, TableLocations> = {
			let mut current_columns: FnvHashMap<u32, usize> = FnvHashMap::default();

			TABLE_INFORMATION_RE_ML
				.captures_iter(s)
				.map(|captures| -> (&str, Vec<TableSegmentSpecifier>) {
					let (name, specs): (&str, &str) = (
						captures
							.name("table")
							.expect("missing capture group table")
							.as_str(),
						captures
							.name("loc")
							.expect("missing capture group loc")
							.as_str(),
					);
					let specs: Vec<&str> = specs.split(' ').collect();
					let specs: Vec<TableSegmentSpecifier> =
						specs.iter().filter_map(|s| s.parse().ok()).collect();
					(name, specs)
				})
				.map(
					|(name, specs): (&str, Vec<TableSegmentSpecifier>)| -> (Table, TableLocations) {
						let table: Table = match (schema, name) {
							(Schema::Census2010(census2010::Schema::Pl94_171), "p1") => {
								Table::Census2010(census2010::Table::Pl94_171(census2010::pl94_171::P1))
							}
							(Schema::Census2010(census2010::Schema::Pl94_171), "p2") => {
								Table::Census2010(census2010::Table::Pl94_171(census2010::pl94_171::P2))
							}
							(Schema::Census2010(census2010::Schema::Pl94_171), "p3") => {
								Table::Census2010(census2010::Table::Pl94_171(census2010::pl94_171::P3))
							}
							(Schema::Census2010(census2010::Schema::Pl94_171), "p4") => {
								Table::Census2010(census2010::Table::Pl94_171(census2010::pl94_171::P4))
							}
							(Schema::Census2010(census2010::Schema::Pl94_171), "h1") => {
								Table::Census2010(census2010::Table::Pl94_171(census2010::pl94_171::H1))
							}
							(Schema::Census2010(census2010::Schema::Pl94_171), _) => unimplemented!(),
							(_, _) => unimplemented!(),
						};

						let locations: TableLocations = specs
							.iter()
							.map(|specifier| {
								if !current_columns.get(&specifier.file).is_some() {
									current_columns.insert(specifier.file, 5_usize);
								}

								let start: usize = *current_columns.get(&specifier.file).unwrap();
								let end: usize = start + specifier.columns;

								current_columns.insert(specifier.file, end);

								TableSegmentLocation {
									file: specifier.file,
									range: start..end,
								}
							})
							.collect();

						(table, locations)
					},
				)
				.collect()
		};

		Ok(Self {
			schema,
			directory: None,
			table_locations,
			tabular_files,
			geographical_header_file,
		})
	}
}

impl PackingList {
	pub fn from_file<P: AsRef<Path>>(file_path: P) -> Result<Self> {
		// It's generally quite a bit faster to just load the entire packing list
		// to a file and then do in-memory operations than deal with potential disk
		// buffering issues, so we first load to string.
		let data: String = read_file_to_string(&file_path)?;

		use core::str::FromStr;
		let mut parsed: Self = Self::from_str(&data)?;
		parsed.directory = file_path.as_ref().parent().map(ToOwned::to_owned);

		Ok(parsed)
	}

	pub fn schema(&self) -> Schema {
		self.schema
	}
}

#[cfg(test)]
mod packing_list {
	use super::{PackingList, Schema, Table};
	macro_rules! t_census2010_pl94_171 {
		($filename:literal, $stusab:ident) => {
			#[cfg(test)]
			mod $stusab {
				use super::{PackingList, Schema, Table};

				#[test]
				fn file_parses_and_is_as_expected() {
					let data = include_str!($filename);

					let packing_list: PackingList = data.parse().unwrap();
					assert_eq!(
						packing_list.schema,
						Schema::Census2010(crate::census2010::Schema::Pl94_171)
					);

					assert!(packing_list.tabular_files.len() == 2);

					assert!(packing_list.table_locations.len() == 5);
					assert!(
						packing_list
							.table_locations
							.get(&Table::Census2010(crate::census2010::Table::Pl94_171(
								crate::census2010::pl94_171::Table::P1
							)))
							.expect("missing mapping for c2010-P1")
							== &vec![crate::TableSegmentLocation {
								file: 1,
								range: 5..(5 + 71)
							}]
					);
					assert!(
						packing_list
							.table_locations
							.get(&Table::Census2010(crate::census2010::Table::Pl94_171(
								crate::census2010::pl94_171::Table::P2
							)))
							.expect("missing mapping for c2010-P2")
							== &vec![crate::TableSegmentLocation {
								file: 1,
								range: (5 + 71)..(5 + 71 + 73)
							}]
					);
					assert!(
						packing_list
							.table_locations
							.get(&Table::Census2010(crate::census2010::Table::Pl94_171(
								crate::census2010::pl94_171::Table::P3
							)))
							.expect("missing mapping for c2010-P3")
							== &vec![crate::TableSegmentLocation {
								file: 2,
								range: 5..(5 + 71)
							}]
					);
					assert!(
						packing_list
							.table_locations
							.get(&Table::Census2010(crate::census2010::Table::Pl94_171(
								crate::census2010::pl94_171::Table::P4
							)))
							.expect("missing mapping for c2010-P4")
							== &vec![crate::TableSegmentLocation {
								file: 2,
								range: (5 + 71)..(5 + 71 + 73)
							}]
					);
					assert!(
						packing_list
							.table_locations
							.get(&Table::Census2010(crate::census2010::Table::Pl94_171(
								crate::census2010::pl94_171::Table::H1
							)))
							.expect("missing mapping for c2010-H1")
							== &vec![crate::TableSegmentLocation {
								file: 2,
								range: (5 + 71 + 73)..(5 + 71 + 73 + 3)
							}]
					);
				}
			}
		};
	}

	t_census2010_pl94_171!("t/ak2010.pl.prd.packinglist.txt", ak);
	t_census2010_pl94_171!("t/al2010.pl.prd.packinglist.txt", al);
	t_census2010_pl94_171!("t/ar2010.pl.prd.packinglist.txt", ar);
	t_census2010_pl94_171!("t/az2010.pl.prd.packinglist.txt", az);
	t_census2010_pl94_171!("t/ca2010.pl.prd.packinglist.txt", ca);
	t_census2010_pl94_171!("t/co2010.pl.prd.packinglist.txt", co);
	t_census2010_pl94_171!("t/ct2010.pl.prd.packinglist.txt", ct);
	t_census2010_pl94_171!("t/dc2010.pl.prd.packinglist.txt", dc);
	t_census2010_pl94_171!("t/de2010.pl.prd.packinglist.txt", de);
	t_census2010_pl94_171!("t/fl2010.pl.prd.packinglist.txt", fl);
	t_census2010_pl94_171!("t/ga2010.pl.prd.packinglist.txt", ga);
	t_census2010_pl94_171!("t/hi2010.pl.prd.packinglist.txt", hi);
	t_census2010_pl94_171!("t/ia2010.pl.prd.packinglist.txt", ia);
	t_census2010_pl94_171!("t/id2010.pl.prd.packinglist.txt", id);
	t_census2010_pl94_171!("t/il2010.pl.prd.packinglist.txt", il);
	t_census2010_pl94_171!("t/in2010.pl.prd.packinglist.txt", r#in);
	t_census2010_pl94_171!("t/ks2010.pl.prd.packinglist.txt", ks);
	t_census2010_pl94_171!("t/ky2010.pl.prd.packinglist.txt", ky);
	t_census2010_pl94_171!("t/la2010.pl.prd.packinglist.txt", la);
	t_census2010_pl94_171!("t/ma2010.pl.prd.packinglist.txt", ma);
	t_census2010_pl94_171!("t/md2010.pl.prd.packinglist.txt", md);
	t_census2010_pl94_171!("t/me2010.pl.prd.packinglist.txt", me);
	t_census2010_pl94_171!("t/mi2010.pl.prd.packinglist.txt", mi);
	t_census2010_pl94_171!("t/mn2010.pl.prd.packinglist.txt", mn);
	t_census2010_pl94_171!("t/mo2010.pl.prd.packinglist.txt", mo);
	t_census2010_pl94_171!("t/ms2010.pl.prd.packinglist.txt", ms);
	t_census2010_pl94_171!("t/mt2010.pl.prd.packinglist.txt", mt);
	t_census2010_pl94_171!("t/nc2010.pl.prd.packinglist.txt", nc);
	t_census2010_pl94_171!("t/nd2010.pl.prd.packinglist.txt", nd);
	t_census2010_pl94_171!("t/ne2010.pl.prd.packinglist.txt", ne);
	t_census2010_pl94_171!("t/nh2010.pl.prd.packinglist.txt", nh);
	t_census2010_pl94_171!("t/nj2010.pl.prd.packinglist.txt", nj);
	t_census2010_pl94_171!("t/nm2010.pl.prd.packinglist.txt", nm);
	t_census2010_pl94_171!("t/nv2010.pl.prd.packinglist.txt", nv);
	t_census2010_pl94_171!("t/ny2010.pl.prd.packinglist.txt", ny);
	t_census2010_pl94_171!("t/oh2010.pl.prd.packinglist.txt", oh);
	t_census2010_pl94_171!("t/ok2010.pl.prd.packinglist.txt", ok);
	t_census2010_pl94_171!("t/or2010.pl.prd.packinglist.txt", or);
	t_census2010_pl94_171!("t/pa2010.pl.prd.packinglist.txt", pa);
	t_census2010_pl94_171!("t/pr2010.pl.prd.packinglist.txt", pr);
	t_census2010_pl94_171!("t/ri2010.pl.prd.packinglist.txt", ri);
	t_census2010_pl94_171!("t/sc2010.pl.prd.packinglist.txt", sc);
	t_census2010_pl94_171!("t/sd2010.pl.prd.packinglist.txt", sd);
	t_census2010_pl94_171!("t/tn2010.pl.prd.packinglist.txt", tn);
	t_census2010_pl94_171!("t/tx2010.pl.prd.packinglist.txt", tx);
	t_census2010_pl94_171!("t/ut2010.pl.prd.packinglist.txt", ut);
	t_census2010_pl94_171!("t/va2010.pl.prd.packinglist.txt", va);
	t_census2010_pl94_171!("t/vt2010.pl.prd.packinglist.txt", vt);
	t_census2010_pl94_171!("t/wa2010.pl.prd.packinglist.txt", wa);
	t_census2010_pl94_171!("t/wi2010.pl.prd.packinglist.txt", wi);
	t_census2010_pl94_171!("t/wv2010.pl.prd.packinglist.txt", wv);
	t_census2010_pl94_171!("t/wy2010.pl.prd.packinglist.txt", wy);
}

impl Dataset<FileBackedLogicalRecord> for IndexedDataset {
	/// Retrieve the logical record by number and by table
	fn get_logical_record(&self, number: LogicalRecordNumber) -> Result<FileBackedLogicalRecord> {
		match &self.logical_record_index {
			Some(index) => {
				let records_from_file: BTreeMap<usize, csv::StringRecord> = self
					.files
					.iter()
					.filter(|(file_type, _)| file_type.is_tabular())
					.map(|(fty, file)| -> (usize, csv::StringRecord) {
						let corresponding_logrec_position_index = index.get(&fty).unwrap();
						let offset: u64 = corresponding_logrec_position_index[number]
							.expect("failed to find position for record");

						use std::io::Seek;
						let mut reader = BufReader::new(file);
						reader
							.seek(std::io::SeekFrom::Start(offset))
							.expect("failed to seek to position for record");

						let mut reader = csv::ReaderBuilder::new()
							.has_headers(false)
							.from_reader(reader);
						let mut record = csv::StringRecord::new();
						reader
							.read_record(&mut record)
							.expect("failed to read record");

						(fty.tabular_index().expect("fty is tabular"), record)
					})
					.collect();

				// log::debug!("Read records: {:?}", records_from_file);

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
				Some(OldSchema::Census2010Pl94_171(_)) => {
					FileType::Census2010Pl94_171(crate::census2010::pl94_171::FileType::GeographicalHeader)
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
					crate::census2010::pl94_171::GeographicalHeader::new(line),
				)),
			}
		} else {
			unimplemented!()
		}
	}
}

lazy_static::lazy_static! {
	static ref TABLE_INFORMATION_RE: Regex =
		Regex::new(r"^(?P<table>[A-Za-z0-9]+)\|(?P<loc>[\d: ]+)\|$")
			.expect("regex parse failed");

	static ref TABLE_INFORMATION_RE_ML: Regex =
		Regex::new(r"(?m)^(?P<table>[A-Za-z0-9]+)\|(?P<loc>[\d: ]+)\|$")
			.expect("regex parse failed");

	static ref FILE_INFORMATION_RE: Regex =
		Regex::new(r"^(?P<filename>(?P<stusab>[a-z]{2})(?P<ident>\w+)(?P<year>\d{4})\.(?P<ds>.+))\|(?P<date>.+)\|(?P<size>\d+)\|(?P<lines>\d+)\|$")
			.expect("regex parse failed");

	static ref FILE_INFORMATION_RE_ML: Regex =
		Regex::new(r"(?m)^(?P<filename>(?P<stusab>[a-z]{2})(?P<ident>\w+)(?P<year>\d{4})\.(?P<ds>.+))\|(?P<date>.+)\|(?P<size>\d+)\|(?P<lines>\d+)\|$")
			.expect("regex parse failed");

	static ref STUSAB_RE: Regex =
		Regex::new(r"(?m)STUSAB: (?P<stusab>[A-Z]{2})$")
			.expect("regex parse failed");
}

#[derive(Clone, Debug, PartialEq)]
enum Line {
	DataSegmentationInformation(TableName, TableLocationSpecifier),
	FileInformation(PathBuf, OldSchema, String),
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

				let schema: OldSchema = match (year.as_str(), ds.as_str()) {
					("2010", "pl") => OldSchema::Census2010Pl94_171(None),
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

		log::debug!("Parsing packing list information from {}", &path);

		let lines = stream
			.lines()
			.map(|maybe_line| maybe_line.expect("couldn't read line"));

		let lines: Vec<Line> = lines
			.flat_map(|line: String| -> Option<Line> {
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
					(OldSchema::Census2010Pl94_171(None), "geo") => {
						FileType::Census2010Pl94_171(census2010::pl94_171::FileType::GeographicalHeader)
					}
					(OldSchema::Census2010Pl94_171(None), maybe_numeric) => FileType::Census2010Pl94_171(
						census2010::pl94_171::Tabular(maybe_numeric.parse::<usize>().unwrap()),
					),
					_ => unimplemented!(),
				};

				log::trace!(" -> file_type = {:?}", file_type);

				if self.schema.is_none() {
					let dataset_schema = match file_type {
						FileType::Census2010Pl94_171(_) => OldSchema::Census2010Pl94_171(None),
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
		let mut current_column_numbers: FnvHashMap<u32, usize> = FnvHashMap::default();

		for line in &lines {
			if let Line::DataSegmentationInformation(table_name, table_location) = line {
				log::trace!("Processing Data Segmentation line: {:?}", line);

				let schema = match (self.schema, table_name.as_str()) {
					(Some(OldSchema::Census2010Pl94_171(None)), "p1") => {
						OldSchema::Census2010Pl94_171(Some(census2010::pl94_171::P1))
					}
					(Some(OldSchema::Census2010Pl94_171(None)), "p2") => {
						OldSchema::Census2010Pl94_171(Some(census2010::pl94_171::P2))
					}
					(Some(OldSchema::Census2010Pl94_171(None)), "p3") => {
						OldSchema::Census2010Pl94_171(Some(census2010::pl94_171::P3))
					}
					(Some(OldSchema::Census2010Pl94_171(None)), "p4") => {
						OldSchema::Census2010Pl94_171(Some(census2010::pl94_171::P4))
					}
					(Some(OldSchema::Census2010Pl94_171(None)), "h1") => {
						OldSchema::Census2010Pl94_171(Some(census2010::pl94_171::H1))
					}
					(Some(OldSchema::Census2010Pl94_171(Some(_))), _) => {
						panic!("schema contains table information")
					}
					(Some(OldSchema::Census2010Pl94_171(None)), table) => {
						panic!("unrecognized table {}", table)
					}
					(None, _) => panic!("schema unknown"),
				};

				let location_specifiers: &Vec<TableSegmentSpecifier> = &table_location;

				let mut locations: Vec<TableSegmentLocation> = Vec::new();

				for table_segment_spec in location_specifiers {
					let file_number: u32 = table_segment_spec.file;

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

					let mut index = LogicalRecordPositionIndex::new_with_size(1_250_000);

					log::trace!("Reading records");

					let t0 = std::time::Instant::now();
					for record in file_reader.records() {
						let record: csv::StringRecord = record?;
						let position = record.position().expect("couldn't find position of record");

						let byte_offset: u64 = position.byte();
						let logrecno: LogicalRecordNumber = record[4]
							.parse::<LogicalRecordNumber>()
							.expect("couldn't parse logical record number");

						index.insert(logrecno, byte_offset);
					}

					log::trace!(
						"Finished indexing in {}ns",
						std::time::Instant::now().duration_since(t0).as_nanos()
					);

					log::trace!("Adding logical record index to global index");

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

impl Default for IndexedDataset {
	fn default() -> Self {
		Self {
			identifier: "".to_string(),
			logical_record_index: None,
			header_index: None,
			schema: None,
			tables: FnvHashMap::default(),
			files: FnvHashMap::default(),
		}
	}
}
