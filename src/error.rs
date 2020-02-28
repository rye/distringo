use std::io;
use core::result;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
	Io(io::Error),
}

pub type Result<T> = result::Result<T, Error>;

impl From<io::Error> for Error {
	fn from(e: io::Error) -> Error {
		Self::Io(e)
	}
}
