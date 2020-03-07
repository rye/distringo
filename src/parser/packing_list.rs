use crate::error::Result;
use core::fmt::Display;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use log::debug;
use regex::Regex;

#[derive(Debug)]
struct SegmentCellDescriptor {
	table: String,
	file: usize,
	width: usize,
}

#[derive(Debug)]
pub struct PackingList {
	segments: Vec<SegmentCellDescriptor>,
}

use lazy_static::lazy_static;

lazy_static! {
	static ref DATA_SEGMENTATION_INFORMATION: Regex =
		Regex::new("^(?P<table>[a-z0-9]+)\\|(?P<descriptor>[\\d: ]*)\\|$").unwrap();
}

impl PackingList {
	pub fn from_file<P: AsRef<Path> + Display + Sized>(file: P) -> Result<PackingList> {
		debug!("Loading packing list from {}", file);

		let file: File = File::open(file)?;
		let stream = BufReader::new(file);

		let segments: Vec<SegmentCellDescriptor> = stream
			.lines()
			.filter_map(std::io::Result::ok)
			.filter_map(|line: String| -> Option<SegmentCellDescriptor> {
				let caps = DATA_SEGMENTATION_INFORMATION.captures(&line)?;

				let (file, width): (usize, usize) = caps["descriptor"]
					.split(" ")
					.map(|chunk: &str| -> Result<(usize, usize)> {
						let file: usize = chunk.split(":").collect::<Vec<&str>>()[0].parse()?;
						let width: usize = chunk.split(":").collect::<Vec<&str>>()[1].parse()?;
						Ok((file, width))
					})
					.filter_map(Result::ok)
					.nth(0)
					// TODO improve error handling
					.expect("could not find an acceptable file:width pairing");

				Some(SegmentCellDescriptor {
					table: caps["table"].to_string(),
					file,
					width,
				})
			})
			.collect();

		Ok(PackingList { segments })
	}
}
