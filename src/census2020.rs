pub mod pl94_171;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Schema {
	Pl94_171,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Table {
	Pl94_171(pl94_171::Table),
}
