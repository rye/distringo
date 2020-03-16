use crate::error::Result;
use crate::parser::packing_list::PackingList;
use crate::schema::CensusData;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub struct Dataset {
	schema: CensusData,
	packing_list: PackingList,
	readers: HashMap<PathBuf, csv::Reader<std::fs::File>>,
	indexes: HashMap<PathBuf, csv_index::RandomAccessSimple<std::fs::File>>,
}

impl Dataset {
	pub fn get_logical_record(&mut self, path: &PathBuf, logrecno: u64) -> Result<csv::StringRecord> {
		let idx: &mut csv_index::RandomAccessSimple<std::fs::File> = self
			.indexes
			.get_mut(path)
			.expect("couldn't find index for file");
		let rdr: &mut csv::Reader<std::fs::File> = self
			.readers
			.get_mut(path)
			.expect("couldn't find reader for file");

		if let Ok(pos) = idx.get(logrecno - 1) {
			rdr.seek(pos)?;
			if let Some(record) = rdr.records().next() {
				Ok(record?)
			} else {
				Err(crate::error::Error::InvalidLogicalRecordNumber)
			}
		} else {
			Err(crate::error::Error::InvalidLogicalRecordNumber)
		}
	}

	pub fn load<P: AsRef<Path>>(packing_list_file: P) -> Result<Self> {
		let packing_list: PackingList = PackingList::from_file(&packing_list_file)?;

		let schema: CensusData = packing_list.schema();

		let mut readers: HashMap<PathBuf, csv::Reader<std::fs::File>> = HashMap::new();
		let mut indexes: HashMap<PathBuf, csv_index::RandomAccessSimple<std::fs::File>> =
			HashMap::new();

		for file in packing_list.files() {
			if file.is_tabular() {
				let dataset_file = file.filename();

				let index = {
					let mut path: PathBuf = dataset_file.clone();

					let mut extension: std::ffi::OsString =
						path.extension().unwrap_or(std::ffi::OsStr::new("")).into();
					extension.push(".idx");

					path.set_extension(extension);

					path
				};

				let mut reader = csv::Reader::from_reader(std::fs::File::open(dataset_file)?);

				if let Ok(file) = std::fs::File::open(index.clone()) {
					log::debug!("{:?}: Already have a valid index", dataset_file);
					indexes.insert(
						dataset_file.clone(),
						csv_index::RandomAccessSimple::open(file)?,
					);
				} else {
					log::debug!("Indexing {:?} into {:?}", dataset_file, index);

					{
						let output = std::fs::File::create(index.clone())?;
						csv_index::RandomAccessSimple::create(&mut reader, output)?;
					}

					let file = std::fs::File::open(index.clone())?;
					indexes.insert(
						dataset_file.clone(),
						csv_index::RandomAccessSimple::open(file)?,
					);
				}

				readers.insert(dataset_file.clone(), reader);
			}
		}

		Ok(Dataset {
			schema,
			packing_list,
			indexes,
			readers,
		})
	}
}
