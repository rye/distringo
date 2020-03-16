use crate::error::Result;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use log::debug;
use regex::Regex;

pub(crate) type SegmentedFileIndex = u32;

#[derive(Clone, Debug)]
pub(crate) struct SegmentationInformation {
	table: String,
	pub(crate) file_width: Vec<(SegmentedFileIndex, usize)>,
}

#[derive(Clone, Debug, PartialEq)]
enum FileType {
	HeaderFile(String),
	TabularFile(SegmentedFileIndex),
}

#[derive(Clone, Debug)]
pub(crate) struct DatasetFile {
	filename: PathBuf,
	extension: String,
	stusab: String,
	descriptor: String,
	year: String,
	ty: FileType,
}

impl DatasetFile {
	pub(crate) fn is_tabular(&self) -> bool {
		match self.ty {
			FileType::TabularFile(_) => true,
			_ => false,
		}
	}

	pub(crate) fn is_header(&self) -> bool {
		match self.ty {
			FileType::HeaderFile(_) => true,
			_ => false,
		}
	}

	pub(crate) fn maybe_tabular_idx(&self) -> Option<(SegmentedFileIndex, PathBuf)> {
		match self.ty {
			FileType::TabularFile(n) => Some((n, self.filename.clone())),
			_ => None,
		}
	}

	pub(crate) fn filename(&self) -> &PathBuf {
		&self.filename
	}

	fn schema(&self) -> crate::schema::CensusData {
		match (&self.year[..], &self.extension[..]) {
			("2010", "pl") => crate::schema::CensusData::Census2010Pl94_171,
			(_, _) => unimplemented!(),
		}
	}
}

#[derive(Debug)]
pub struct PackingList {
	schema: crate::schema::CensusData,
	files: Vec<DatasetFile>,
	tables: Vec<(String, SegmentationInformation)>,
}

impl PackingList {
	pub fn schema(&self) -> crate::schema::CensusData {
		self.schema
	}

	pub(crate) fn files(&self) -> &Vec<DatasetFile> {
		&self.files
	}

	pub(crate) fn tables(&self) -> &Vec<(String, SegmentationInformation)> {
		&self.tables
	}

	pub(crate) fn header_file(&self) -> PathBuf {
		let header = self
			.files
			.iter()
			.find(|f| f.is_header())
			.expect("couldn't find a header file");
		header.filename.clone()
	}

	pub(crate) fn tabular_files(&self) -> BTreeMap<SegmentedFileIndex, PathBuf> {
		self
			.files
			.iter()
			.flat_map(crate::parser::packing_list::DatasetFile::maybe_tabular_idx)
			.collect()
	}
}

use lazy_static::lazy_static;

lazy_static! {
	static ref SEGMENTATION_INFORMATION: Regex =
		Regex::new("^(?P<table>[a-z0-9]+)\\|(?P<descriptor>[\\d: ]*)\\|$").unwrap();
	static ref FILE_INFORMATION: Regex =
		Regex::new("^(?P<filename>.*)\\|(?P<date>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\|(?P<size>\\d+)\\|(?P<rows>\\d+)\\|$)").unwrap();
	static ref FILE_NAME: Regex =
		Regex::new("^(?P<stusab>[a-z]{2})(?P<descriptor>.*)(?P<year>\\d{4})\\.(?P<type>.*)$").unwrap();
}

enum Line {
	DataSegmentation(SegmentationInformation),
	FileInformation(DatasetFile),
	None,
}

impl core::convert::TryFrom<String> for SegmentationInformation {
	type Error = crate::error::Error;

	fn try_from(line: String) -> crate::error::Result<Self> {
		let caps = SEGMENTATION_INFORMATION
			.captures(&line)
			.ok_or(crate::error::Error::ParsePackingListLine)?;

		let file_width = caps["descriptor"]
			.split(' ')
			.map(|chunk: &str| -> Result<(SegmentedFileIndex, usize)> {
				let file: SegmentedFileIndex = chunk.split(':').collect::<Vec<&str>>()[0].parse()?;
				let width: usize = chunk.split(':').collect::<Vec<&str>>()[1].parse()?;
				Ok((file, width))
			})
			.filter_map(Result::ok)
			.collect();

		Ok(SegmentationInformation {
			table: caps["table"].to_string(),
			file_width,
		})
	}
}

impl core::convert::TryFrom<String> for DatasetFile {
	type Error = crate::error::Error;

	fn try_from(line: String) -> crate::error::Result<Self> {
		let caps = FILE_INFORMATION
			.captures(&line)
			.ok_or(crate::error::Error::ParsePackingListLine)?;

		let filename: String = caps["filename"].to_string();

		let filename_caps = FILE_NAME
			.captures(&filename)
			.ok_or(crate::error::Error::ParsePackingListFilename)?;

		let stusab: String = filename_caps["stusab"].to_string();
		let descriptor: String = filename_caps["descriptor"].to_string();
		let year: String = filename_caps["year"].to_string();
		let extension: String = filename_caps["type"].to_string();

		let ty: FileType = match descriptor.parse::<SegmentedFileIndex>() {
			Ok(index) => FileType::TabularFile(index),
			Err(_) => FileType::HeaderFile(descriptor.clone()),
		};

		let filename: PathBuf = filename.into();

		Ok(DatasetFile {
			filename,
			descriptor,
			ty,
			year,
			stusab,
			extension,
		})
	}
}

impl From<String> for Line {
	fn from(line: String) -> Self {
		use core::convert::TryInto;
		if SEGMENTATION_INFORMATION.is_match(&line) {
			debug!("Interpreting \"{}\" as Data Segmentation", line);
			line
				.try_into()
				.map(Self::DataSegmentation)
				.unwrap_or(Self::None)
		} else if FILE_INFORMATION.is_match(&line) {
			debug!("Interpreting line \"{}\" as File Information", line);
			line
				.try_into()
				.map(Self::FileInformation)
				.unwrap_or(Self::None)
		} else {
			Self::None
		}
	}
}

impl PackingList {
	pub fn from_file<P: AsRef<Path> + Sized>(file: P) -> Result<PackingList> {
		let file: File = File::open(file)?;
		let stream = BufReader::new(file);

		let lines: Vec<Line> = stream
			.lines()
			.filter_map(|line| line.ok().map(Into::into))
			.collect();

		let files: Vec<DatasetFile> = {
			lines
				.iter()
				.filter_map(|line: &Line| -> Option<&DatasetFile> {
					match line {
						Line::FileInformation(file) => Some(&file),
						_ => None,
					}
				})
				.cloned()
				.collect()
		};

		let schemas: std::collections::BTreeSet<crate::schema::CensusData> = files
			.iter()
			.map(|file: &DatasetFile| -> crate::schema::CensusData { file.schema() })
			.collect();

		debug_assert!(schemas.len() == 1);

		let schema: crate::schema::CensusData =
			*schemas.iter().next().expect("couldn't infer a schema");

		let tables: Vec<(String, SegmentationInformation)> = {
			lines
				.iter()
				.flat_map(|line: &Line| -> Option<&SegmentationInformation> {
					match line {
						Line::DataSegmentation(info) => Some(info),
						_ => None,
					}
				})
				.cloned()
				.map(|si| -> (String, SegmentationInformation) { (si.table.clone(), si) })
				.collect()
		};

		Ok(PackingList {
			schema,
			files,
			tables,
		})
	}
}
