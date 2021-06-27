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

#[derive(Debug, Clone)]
pub struct IndexedDataset<Key> {
	header_file: std::ffi::OsString,
	tabular_files: Vec<std::ffi::OsString>,
	indexes: std::collections::HashMap<std::ffi::OsString, fnv::FnvHashMap<Key, usize>>,
}

use thiserror::Error;

#[derive(Debug, Copy, Clone, Error)]
enum DatasetError {
	#[error("zero passed as tabular index")]
	ZeroPassed,
}

impl Dataset {
	pub fn new() -> Self {
		Self { ..Default::default() }
	}

	pub fn header_file(mut self, header_file: std::path::PathBuf) -> anyhow::Result<Self> {
		self.header_file = Some(header_file);
		Ok(self)
	}

	pub fn tabular_file(mut self, index: usize, tabular_file: std::path::PathBuf) -> anyhow::Result<Self> {
		let index = index.checked_sub(1).ok_or(DatasetError::ZeroPassed)?;

		if index > self.tabular_files.len() {
			self.tabular_files.resize(index, None);
		}

		self.tabular_files.insert(index, Some(tabular_file));

		Ok(self)
	}

	pub fn index<Key>(mut self) -> anyhow::Result<IndexedDataset<Key>> {


	}
}
