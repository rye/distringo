use std::fs::File;
use std::path::{Path, PathBuf};

use fnv::FnvHashMap;
use regex::Regex;

use super::indexed::{FILE_INFORMATION_RE_ML, STUSAB_RE, TABLE_INFORMATION_RE_ML};

use crate::census2010;
use crate::Result;
use crate::Schema;
use crate::Table;
use crate::TableLocations;
use crate::TableSegmentLocation;
use crate::TableSegmentSpecifier;

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

impl core::str::FromStr for PackingList {
	type Err = crate::error::Error;

	fn from_str(s: &str) -> Result<Self> {
		log::debug!("Parsing packing list (of {} bytes)", s.len());

		// PHASE 0: Build the regex for the stusab

		log::debug!("Parsing STUSAB field from packing list data");

		let stusab: String = {
			let stusab_all_caps: &str = &STUSAB_RE.captures(s).expect("failed to find STUSAB")["stusab"];
			let stusab: String = stusab_all_caps.to_lowercase();
			stusab
		};

		log::debug!("Inferred STUSAB: {}", stusab);

		// PHASE 1: Collect the Schema

		log::trace!("Compiling filename regex");

		// TODO rather than building this regex at parse time, maybe store a lazy_static cache of these for each stusab somewhere?
		let filename_re: Regex = Regex::new(&format!(
			r"(?m){}(?P<inner>\w*)(?P<year>\d{{4}})\.(?P<ext>[a-z1-9\-]*)\b",
			stusab
		))
		.expect("failed to create generated regex");

		log::trace!("Finished compiling filename regex");

		log::debug!("Inferring schema");

		let mut schemas: Vec<Schema> = filename_re
			.captures_iter(s)
			.map(|captures| {
				log::trace!(
					"Processing filename regex match: {}",
					captures.get(0).unwrap().as_str()
				);

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

		log::trace!("Deduplicating {} schemas", schemas.len());

		schemas.dedup();

		log::trace!("Now have {} schema(s)", schemas.len());

		assert_eq!(schemas.len(), 1);

		let schema: Schema = schemas.remove(0);

		log::debug!("Inferred schema: {:?}", schema);

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

		log::debug!("Reading packing list content definitions");

		let (tabular_files, geographical_header_file): (FnvHashMap<u32, PathBuf>, PathBuf) = {
			let file_informations: Vec<FileInformation> = FILE_INFORMATION_RE_ML
				.captures_iter(s)
				.map(|captures| {
					log::trace!(
						"Processing file information regex match: {}",
						captures.get(0).unwrap().as_str()
					);

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

					log::trace!("Inferred filetype {:?} for {:?}", ty, filename);

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

		log::debug!("Reading data segmentation specifiers");

		// TODO consider just hard-coding the table locations in our spec

		let table_locations: FnvHashMap<Table, TableLocations> = {
			let mut current_columns: FnvHashMap<u32, usize> = FnvHashMap::default();

			TABLE_INFORMATION_RE_ML
				.captures_iter(s)
				.map(|captures| -> (Table, TableLocations) {
					log::trace!(
						"Processing table segmentation regex match: {}",
						captures.get(0).unwrap().as_str()
					);

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
							if current_columns.get(&specifier.file).is_none() {
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

					log::trace!("Table {:?} is found at {:?}", table, locations);

					(table, locations)
				})
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

#[cfg(test)]
mod tests {
	use crate::{Schema, Table};

	use super::PackingList;

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
