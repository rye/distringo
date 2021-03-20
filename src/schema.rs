use crate::census2010;
use crate::census2020;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum Schema {
	Census2010(census2010::Schema),
	Census2020(census2020::Schema),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum Table {
	Census2010(census2010::Table),
	Census2020(census2020::Table),
}
