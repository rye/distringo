use core::result;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {}

pub type Result<T> = result::Result<T, Error>;
