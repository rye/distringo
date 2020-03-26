use core::{num, result};
use std::io;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
	Io(io::Error),
	Csv(csv::Error),
	Config(config::ConfigError),
	ParseInt(num::ParseIntError),

	Parse(ParseError),

	InvalidServerHost,
	InvalidServerPort,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ParseError {
	// PackingList(PackingListParseError),
}

// #[derive(Debug)]
// #[non_exhaustive]
// pub enum PackingListParseError {
// 	Malformed
// }

pub type Result<T> = result::Result<T, Error>;

impl From<io::Error> for Error {
	fn from(e: io::Error) -> Error {
		Self::Io(e)
	}
}

impl From<csv::Error> for Error {
	fn from(e: csv::Error) -> Error {
		Self::Csv(e)
	}
}

impl From<config::ConfigError> for Error {
	fn from(e: config::ConfigError) -> Error {
		Self::Config(e)
	}
}

impl From<num::ParseIntError> for Error {
	fn from(e: num::ParseIntError) -> Error {
		Self::ParseInt(e)
	}
}
