#![allow(dead_code)]
#![allow(unused_variables)]

use std::io::{BufRead, BufReader};
use std::path::Path;

pub mod error;

/// A Census Dataset
///
/// Every dataset has a unique, human-identifiable identifier, which is used
/// internally for reading the data.
pub struct Dataset {
	schema: Box<dyn Schema>,
	packing_list: PackingList,
	index: Option<LogicalRecordIndex>,
}

pub struct LogicalRecordIndex {}

pub trait Table {}

pub struct TableLocation {}

pub struct PackingList(std::collections::HashMap<Box<dyn Table>, TableLocation>);

impl PackingList {
	pub fn from_file<P: core::fmt::Display + AsRef<Path>>(path: P) -> crate::error::Result<Self> {
		log::debug!("Opening {} for reading", &path);

		let file = std::fs::File::open(&path)?;
		let stream = BufReader::new(file);

		log::debug!("Successfully opened {} for reading", &path);

		// Stream -> Sections

		let delimiter: String = "#".repeat(80);
		let lines: Vec<String> = stream
			.lines()
			.map(|r| r.expect("couldn't parse line"))
			.collect();
		let sections = lines
			.split(|line| line == &"#".repeat(80) || line == &"#".repeat(81))
			.filter(|section| section.len() > 0 && !(section.iter().all(|line| line.trim().len() == 0)));

		for section in sections {
			log::trace!("Section: {:#?}", section);
		}

		// Sections -> Data

		// Data -> Result<Self>

		unimplemented!()
	}
}

pub mod census2010 {
	pub enum DataSchema {
		Pl94_171,
	}

	impl crate::Schema for DataSchema {}

	pub mod pl94_171 {
		pub enum Table {
			P1,
			P2,
			P3,
			P4,
			H1,
		}

		impl crate::Table for Table {}
	}
}

pub trait Schema {}

impl Dataset {
	pub fn read_packing_list<P: core::fmt::Display + AsRef<Path>>(
		path: P,
	) -> crate::error::Result<Self> {
		let packing_list = PackingList::from_file(path)?;

		unimplemented!()
	}

	pub fn generate_index(&self) -> crate::error::Result<()> {
		unimplemented!()
	}

	pub fn get_logical_record(&self, logical_record_number: u32) -> crate::error::Result<()> {
		unimplemented!()
	}
}
