use std::io;
use core::{num, result};

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
	Io(io::Error),
	ParseInt(num::ParseIntError),
}

pub type Result<T> = result::Result<T, Error>;

impl From<io::Error> for Error {
	fn from(e: io::Error) -> Error {
		Self::Io(e)
	}
}

impl From<num::ParseIntError> for Error {
	fn from(e: num::ParseIntError) -> Error {
		Self::ParseInt(e)
	}
}
