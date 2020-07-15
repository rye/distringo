use core::{num, result};
use std::io;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
	Io(io::Error),
	Csv(csv::Error),
	Config(config::ConfigError),
	GeoJson(geojson::Error),
	ParseInt(num::ParseIntError),

	InvalidServerHost,
	InvalidServerPort,
}

impl core::fmt::Display for Error {
	fn fmt(&self, f: &mut core::fmt::Formatter) -> core::result::Result<(), std::fmt::Error> {
		match self {
			Error::Io(inner) => writeln!(f, "io error: {}", inner),
			Error::Csv(inner) => writeln!(f, "csv error: {}", inner),
			Error::Config(inner) => writeln!(f, "config error: {}", inner),
			Error::GeoJson(inner) => writeln!(f, "geojson error: {}", inner),
			_ => todo!(),
		}
	}
}

pub type Result<T> = result::Result<T, Error>;

impl std::error::Error for Error {}

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

impl From<geojson::Error> for Error {
	fn from(e: geojson::Error) -> Error {
		Self::GeoJson(e)
	}
}

impl From<num::ParseIntError> for Error {
	fn from(e: num::ParseIntError) -> Error {
		Self::ParseInt(e)
	}
}
