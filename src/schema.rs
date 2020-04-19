use crate::census2010;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Schema {
	Census2010Pl94_171(Option<census2010::pl94_171::Table>),
}

impl<S: AsRef<str>> core::convert::From<S> for Schema {
	fn from(s: S) -> Self {
		let s: &str = s.as_ref();
		match s {
			"p1" => Schema::Census2010Pl94_171(Some(census2010::pl94_171::P1)),
			"p2" => Schema::Census2010Pl94_171(Some(census2010::pl94_171::P2)),
			"p3" => Schema::Census2010Pl94_171(Some(census2010::pl94_171::P3)),
			"p4" => Schema::Census2010Pl94_171(Some(census2010::pl94_171::P4)),
			"h1" => Schema::Census2010Pl94_171(Some(census2010::pl94_171::H1)),
			_ => unimplemented!(),
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::census2010::pl94_171::Table;
	use crate::Schema;

	#[test]
	fn schema_with_table_de() {
		let data = r"Census2010Pl94_171: P1";
		let schema: Schema = serde_yaml::from_str(data).unwrap();
		assert_eq!(schema, Schema::Census2010Pl94_171(Some(Table::P1)))
	}

	#[test]
	fn bare_schema_de() {
		let data = r"Census2010Pl94_171:";
		let schema: Schema = serde_yaml::from_str(data).unwrap();
		assert_eq!(schema, Schema::Census2010Pl94_171(None))
	}
}
