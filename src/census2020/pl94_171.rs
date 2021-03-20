use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Table {
	P1,
	P2,
	P3,
	P4,
	H1,
	P5,
}

pub use Table::{H1, P1, P2, P3, P4, P5};
