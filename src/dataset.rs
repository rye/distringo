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
