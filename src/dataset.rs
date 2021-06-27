//! Dataset structures and year/dataset-specific implementations thereof
//!
//! This module contains the core generics and expectations, and then some of the implementations
//! of those generics.

/// A type that can locate, _on a given line_, where a field lives.
trait DelimiterlessLayout<'input>: Layout<'input> {
	fn get_span(field: &Self::Field) -> core::ops::Range<usize>;
}

/// A type that can locate,
trait ColumnLayout<F: header::Field> {
	fn locate_field(&self, field: &F) -> usize;
}

trait Layout<'input> {
	type Field;

	fn all_fields(data: &'input str) -> Vec<(&Self::Field, &'input str)>;
	fn all_fields_trimmed(data: &'input str) -> Vec<(&Self::Field, &'input str)>;
	fn fields(data: &'input str) -> Vec<(&Self::Field, &'input str)>;
}

mod census2010;
mod header;

#[derive(Debug, Clone, Default)]
pub struct Dataset {
	header_file: Option<std::path::PathBuf>,
	tabular_files: Vec<Option<std::path::PathBuf>>,
}

#[derive(Debug)]
pub struct IndexedFile<Key: core::cmp::Eq + core::hash::Hash> {
	file: std::fs::File,
	index: fnv::FnvHashMap<Key, usize>,
}

pub struct LogicalRecordOffsetIndex {}

impl core::convert::TryFrom<std::path::PathBuf> for IndexedFile<usize> {
	type Error = anyhow::Error;

	/// Read the file specified in `path` into an IndexedFile.
	///
	/// # Assumptions
	///
	/// This conversion assumes that the lines appear in _logical record order_.
	fn try_from(path: std::path::PathBuf) -> anyhow::Result<Self> {
		let mut file: std::fs::File = std::fs::OpenOptions::new().read(true).open(path)?;

		use std::io::Seek;

		let mut logrecno = 0_usize;

		debug_assert!(file.stream_position()? == 0_u64);

		let mut buf_reader = std::io::BufReader::new(file);

		let mut index: fnv::FnvHashMap<usize, usize> = fnv::FnvHashMap::default();

		loop {
			let mut line: String = String::new();

			use core::convert::TryInto;
			use std::io::BufRead;

			logrecno += 1;
			let offset: usize = buf_reader.stream_position()?.try_into().unwrap();

			let read_size: usize = buf_reader.read_line(&mut line)?;

			if read_size == 0 {
				break;
			}

			index.insert(logrecno, offset);
		}

		Ok(IndexedFile {
			file: buf_reader.into_inner(),
			index,
		})
	}
}

impl<Key: core::cmp::Eq + core::hash::Hash> IndexedFile<Key> {
	pub fn get_line(&self, key: Key) -> Option<String> {
		use std::io::{BufRead, BufReader, Seek, SeekFrom};

		let mut reader = BufReader::new(&self.file);
		let mut buf: String = String::new();
		let offset = self.index.get(&key)?;

		reader.seek(SeekFrom::Start(*offset as u64)).ok()?;
		reader.read_line(&mut buf).ok()?;

		Some(buf)
	}
}

#[derive(Debug)]
pub struct IndexedDataset<Key: core::cmp::Eq + core::hash::Hash> {
	header_file: IndexedFile<Key>,
	tabular_files: Vec<IndexedFile<Key>>,
}

impl<Key: core::cmp::Eq + core::hash::Hash> IndexedDataset<Key> {
	pub fn header_file(&self) -> &IndexedFile<Key> {
		&self.header_file
	}
}

impl<Key: core::cmp::Eq + core::hash::Hash> IndexedDataset<Key> {
	pub fn tabular_file(&self, number: usize) -> Option<&IndexedFile<Key>> {
		number
			.checked_sub(1)
			.map(|n| self.tabular_files.get(n))
			.flatten()
	}
}

#[derive(Debug, Copy, Clone, thiserror::Error)]
pub enum DatasetError {
	#[error("zero passed as tabular index")]
	ZeroPassed,
	#[error("no header file specified")]
	NoHeader,
	#[error("tabular file list discontinuity (did you pass exactly 1..n with no gaps?)")]
	NoneTabularPathFound,
}

impl Dataset {
	pub fn new() -> Self {
		Self {
			..Default::default()
		}
	}

	pub fn header_file(mut self, header_file: std::path::PathBuf) -> anyhow::Result<Self> {
		self.header_file = Some(header_file);
		Ok(self)
	}

	pub fn tabular_file(
		mut self,
		index: usize,
		tabular_file: std::path::PathBuf,
	) -> anyhow::Result<Self> {
		let index = index.checked_sub(1).ok_or(DatasetError::ZeroPassed)?;

		if index > self.tabular_files.len() {
			self.tabular_files.resize(index, None);
		}

		self.tabular_files.insert(index, Some(tabular_file));

		Ok(self)
	}

	#[must_use = "consumes the source Dataset and indexes it"]
	pub fn index(self) -> anyhow::Result<IndexedDataset<usize>> {
		use core::convert::TryInto;

		let header_file: IndexedFile<usize> =
			self.header_file.ok_or(DatasetError::NoHeader)?.try_into()?;
		let tabular_files: anyhow::Result<Vec<IndexedFile<usize>>> = self
			.tabular_files
			.into_iter()
			.map(
				|opt: Option<std::path::PathBuf>| -> anyhow::Result<IndexedFile<usize>> {
					match opt {
						Some(pathbuf) => pathbuf.try_into(),
						None => Err(anyhow::anyhow!(DatasetError::NoneTabularPathFound)),
					}
				},
			)
			.collect();

		let tabular_files: Vec<IndexedFile<usize>> = tabular_files?;

		Ok(IndexedDataset {
			header_file,
			tabular_files,
		})
	}
}

#[cfg(test)]
mod tests {}
