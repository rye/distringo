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

macro_rules! generate_field_getter {
	($container_type:ty, $container_data_field:ident, $name:ident, $width:literal, [ $vis:vis , $getter_name:ident #> $pty:ty ]) => {
		#[allow(dead_code)]
		impl $container_type {
			#[must_use]
			$vis fn $getter_name(&self) -> $pty {
				debug_assert!(self.$container_data_field[$name].len() <= $width);
				self.$container_data_field[$name].parse::<$pty>().unwrap()
			}
		}
	};

	($container_type:ty, $container_data_field:ident, $name:ident, $width:literal, [ $vis:vis , $getter_name:ident #> $pty:ty | e.g. $expected:tt ]) => {
		generate_field_getter!($container_type, $container_data_field, $name, $width, [ $vis , $getter_name #> $pty ]);

		#[test]
		fn $getter_name() {
			let geo_header = <$container_type>::new(RI_GEO2018_2020_STYLE_EXAMPLE);
			let expected = $expected;
			assert_eq!(geo_header.$getter_name(), expected);
		}
	};



	($container_type:ty, $container_data_field:ident, $name:ident, $width:literal, [ $vis:vis , $getter_name:ident ]) => {
		#[allow(dead_code)]
		impl $container_type {
			#[must_use]
			$vis fn $getter_name(&self) -> &str {
				debug_assert!(self.$container_data_field[$name].len() <= $width);
				&self.$container_data_field[$name]
			}
		}
	};

	($container_type:ty, $container_data_field:ident, $name:ident, $width:literal, [ $vis:vis , $getter_name:ident e.g. $expected:literal ]) => {
		generate_field_getter!($container_type, $container_data_field, $name, $width, [ $vis , $getter_name ]);

		#[test]
		fn $getter_name() {
			let geo_header = <$container_type>::new(RI_GEO2018_2020_STYLE_EXAMPLE);
			assert_eq!(geo_header.$getter_name(), $expected);
		}
	};


	($container_type:ty, $container_data_field:ident, $name:ident, $width:literal, [ $vis:vis , | $getter_name:ident | ]) => {
		#[allow(dead_code)]
		impl $container_type {
			#[must_use]
			$vis fn $getter_name(&self) -> &str {
				debug_assert!(self.$container_data_field[$name].len() <= $width);
				&self.$container_data_field[$name].trim()
			}
		}
	};

	($container_type:ty, $container_data_field:ident, $name:ident, $width:literal, [ $vis:vis , | $getter_name:ident | e.g. $expected:literal ]) => {
		generate_field_getter!($container_type, $container_data_field, $name, $width, [ $vis , |$getter_name| ]);

		#[test]
		fn $getter_name() {
			let geo_header = <$container_type>::new(RI_GEO2018_2020_STYLE_EXAMPLE);
			assert_eq!(geo_header.$getter_name(), $expected);
		}
	};
}

macro_rules! generate_fields_inner {
	($container_type:ident, $container_data_field:ident, $name:ident, {}) => {};

	($container_type:ident, $container_data_field:ident, $name:ident, { @ + $loc:literal w $width:literal - $rest:tt }) => {
		#[allow(dead_code)]
		const $name: usize = $loc;

		generate_field_getter!($container_type, $container_data_field, $name, $width, $rest);
	};
}

macro_rules! generate_fields {
	($container_type:ident, $container_data_field:ident, $($name:ident $rest:tt),+) => {
		$(
			generate_fields_inner!($container_type, $container_data_field, $name, $rest);
		)+
	};
}

#[allow(dead_code)]
const RI_GEO2018_2020_STYLE_EXAMPLE: &str = "PLST|RI|040|00|00|000|00|0000001|0400000US44|44|1|1|44|01219835|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||1060563703|67858968|Rhode Island|Rhode Island|A||614053|271266|+41.8697678|-071.5786246|00||";

generate_fields!(
	GeographicalHeader, data,

	// Record codes
	FILEID { @ + 0 w 6 - [pub, fileid e.g. "PLST"] },
	STUSAB { @ + 1 w 2 - [pub, stusab e.g. "RI"] },
	SUMLEV { @ + 2 w 3 - [pub, sumlev e.g. "040"] },
	// TODO(rye) 2020 +field: GEOVAR.
	GEOVAR { @ + 3 w 2 - [pub, geovar e.g. "00"] },
	GEOCOMP { @ + 4 w 2 - [pub, geocomp e.g. "00"] },
	CHARITER { @ + 5 w 3 - [pub, chariter e.g. "000"] },
	CIFSN { @ + 6 w 2 - [pub, cifsn e.g. "00"] },
	LOGRECNO { @ + 7 w 7 - [pub, logrecno #> crate::LogicalRecordNumber | e.g. 1_u64] },

	// Geographic Area Codes
	GEOID {},
	GEOCODE {},
	REGION {},
	DIVISION {},
	STATE {},
	STATENS {},
	COUNTY {},
	COUNTYCC {},
	COUNTYNS {},
	COUSUB {},
	COUSUBCC {},
	COUSUBNS {},
	SUBMCD {},
	SUBMCDCC {},
	SUBMCDNS {},
	ESTATE {},
	ESTATECC {},
	ESTATENS {},
	CONCIT {},
	CONCITCC {},
	CONCITNS {},
	PLACE {},
	PLACECC {},
	PLACENS {},
	TRACT {},
	BLKGRP {},
	BLOCK {},
	AIANHH {},
	AIHHTLI {},
	AIANHHFP {},
	AIANHHCC {},
	AIANHHNS {},
	AITS {},
	AITSFP {},
	AITSCC {},
	AITSNS {},
	TTRACT {},
	TBLKGRP {},
	ANRC {},
	ANRCCC {},
	ANRCNS {},
	CBSA {},
	MEMI {},
	CSA {},
	METDIV {},
	NECTA {},
	NMEMI {},
	CNECTA {},
	NECTADIV {},
	CBSAPCI {},
	NECTAPCI {},
	UA {},
	UATYPE {},
	UR {},
	CD116 {},
	CD118 {},
	CD119 {},
	CD120 {},
	CD121 {},
	SLDU18 {},
	SLDU22 {},
	SLDU24 {},
	SLDU26 {},
	SLDU28 {},
	SLDL18 {},
	SLDL22 {},
	SLDL24 {},
	SLDL26 {},
	SLDL28 {},
	VTD {},
	VTDI {},
	ZCTA {},
	SDELM {},
	SDSEC {},
	SDUNI {},
	PUMA {},

	// Area Characteristics
	AREALAND {},
	AREAWATR {},
	BASENAME {},
	NAME {},
	FUNCSTAT {},
	GCUNI {},
	POP100 {},
	HU100 {},
	INTPTLAT {},
	INTPTLON {},
	LSADC {},
	PARTFLAG {},

	// Special Area Codes
	UGA {}
);

pub struct GeographicalHeader {
	data: Vec<String>,
}

impl GeographicalHeader {
	pub fn new(data: &str) -> Self {
		Self {
			data: data.split('|').map(str::to_owned).collect(),
		}
	}
}

impl crate::GeographicalHeader for GeographicalHeader {
	fn name(&self) -> &str {
		&self.name()
	}

	fn logrecno(&self) -> crate::LogicalRecordNumber {
		self.logrecno()
	}
}
