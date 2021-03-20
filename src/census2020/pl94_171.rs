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
const RI_GEO2018_2020_STYLE_EXAMPLE: &str = "PLST|RI|750|00|00|000|00|0019326|7500000US440070185003030|440070185003030|1|1|44|01219835|007|H4|01219781|80780|C5|01220079|||||||99999|99|99999999|80780|C5|01220079|018500|3|3030|9999|9|99999|99|99999999|999|99999|99|99999999|999999|9|99999|99|99999999|39300|1|148|99999|77200|1|715|99999|N|N||||01|||||020|||||051|||||443909|A||99999|99999|01200||1625|0|3030|Block 3030|S||0|0|+41.9866626|-071.4802535|BK||99999";

generate_fields!(
	GeographicalHeader, data,

	// Record codes
	FILEID { @ + 0 w 6 - [pub, fileid e.g. "PLST"] },
	STUSAB { @ + 1 w 2 - [pub, stusab e.g. "RI"] },
	SUMLEV { @ + 2 w 3 - [pub, sumlev e.g. "750"] },
	// TODO(rye) 2020 +field: GEOVAR.
	GEOVAR { @ + 3 w 2 - [pub, geovar e.g. "00"] },
	GEOCOMP { @ + 4 w 2 - [pub, geocomp e.g. "00"] },
	CHARITER { @ + 5 w 3 - [pub, chariter e.g. "000"] },
	CIFSN { @ + 6 w 2 - [pub, cifsn e.g. "00"] },
	LOGRECNO { @ + 7 w 7 - [pub, logrecno #> crate::LogicalRecordNumber | e.g. 19326_u64] },

	// Geographic Area Codes
	GEOID { @ + 8 w 60 - [ pub, geoid e.g. "7500000US440070185003030" ] },
	GEOCODE { @ + 9 w 51 - [ pub, geocode e.g. "440070185003030" ] },
	REGION { @ + 10 w 1 - [ pub, region e.g. "1" ] },
	DIVISION { @ + 11 w 1 - [ pub, division e.g. "1" ] },
	STATE { @ + 12 w 2 - [ pub, state e.g. "44" ] },
	STATENS { @ + 13 w 8 - [ pub, statens e.g. "01219835" ] },
	COUNTY { @ + 14 w 3 - [ pub, county e.g. "007" ] },
	COUNTYCC { @ + 15 w 2 - [ pub, countycc e.g. "H4" ] },
	COUNTYNS { @ + 16 w 8 - [ pub, countyns e.g. "01219781" ] },
	COUSUB { @ + 17 w 5 - [ pub, cousub e.g. "80780" ] },
	COUSUBCC { @ + 18 w 2 - [ pub, cousubcc e.g. "C5" ] },
	COUSUBNS { @ + 19 w 8 - [ pub, cousubns e.g. "01220079" ] },
	SUBMCD { @ + 20 w 5 - [ pub, submcd e.g. "" ] },
	SUBMCDCC { @ + 21 w 2 - [ pub, submcdcc e.g. "" ] },
	SUBMCDNS { @ + 22 w 8 - [ pub, submcdns e.g. "" ] },
	ESTATE { @ + 23 w 5 - [ pub, estate e.g. "" ] },
	ESTATECC { @ + 24 w 2 - [ pub, estatecc e.g. "" ] },
	ESTATENS { @ + 25 w 8 - [ pub, estatens e.g. "" ] },
	CONCIT { @ + 26 w 5 - [ pub, concit e.g. "99999" ] },
	CONCITCC { @ + 27 w 2 - [ pub, concitcc e.g. "99" ] },
	CONCITNS { @ + 28 w 8 - [ pub, concitns e.g. "99999999" ] },
	PLACE { @ + 29 w 5 - [ pub, place e.g. "80780" ] },
	PLACECC { @ + 30 w 2 - [ pub, placecc e.g. "C5" ] },
	PLACENS { @ + 31 w 8 - [ pub, placens e.g. "01220079" ] },
	TRACT { @ + 32 w 6 - [ pub, tract e.g. "018500" ] },
	BLKGRP { @ + 33 w 1 - [ pub, blkgrp e.g. "3" ] },
	BLOCK { @ + 34 w 4 - [ pub, block e.g. "3030" ] },

	AIANHH { @ + 35 w 4 - [ pub, aianhh e.g. "9999" ] },
	AIHHTLI { @ + 36 w 1 - [ pub, aihhtli e.g. "9" ] },
	AIANHHFP { @ + 37 w 5 - [ pub, aianhhfp e.g. "99999" ] },
	AIANHHCC { @ + 38 w 2 - [ pub, aianhhcc e.g. "99" ] },
	AIANHHNS { @ + 39 w 8 - [ pub, aianhhns e.g. "99999999" ] },
	AITS { @ + 40 w 3 - [ pub, aits e.g. "999" ] },
	AITSFP { @ + 41 w 5 - [ pub, aitsfp e.g. "99999" ] },
	AITSCC { @ + 42 w 2 - [ pub, aitscc e.g. "99" ] },
	AITSNS { @ + 43 w 8 - [ pub, aitsns e.g. "99999999" ] },
	TTRACT { @ + 44 w 6 - [ pub, ttract e.g. "999999" ] },
	TBLKGRP { @ + 45 w 1 - [ pub, tblkgrp e.g. "9" ] },
	ANRC { @ + 46 w 5 - [ pub, anrc e.g. "99999" ] },
	ANRCCC { @ + 47 w 2 - [ pub, anrccc e.g. "99" ] },
	ANRCNS { @ + 48 w 8 - [ pub, anrcns e.g. "99999999" ] },
	CBSA { @ + 49 w 5 - [ pub, cbsa e.g. "39300" ] },
	MEMI { @ + 50 w 1 - [ pub, memi e.g. "1" ] },
	CSA { @ + 51 w 3 - [ pub, csa e.g. "148" ] },
	METDIV { @ + 52 w 5 - [ pub, metdiv e.g. "99999" ] },
	NECTA { @ + 53 w 5 - [ pub, necta e.g. "77200" ] },
	NMEMI { @ + 54 w 1 - [ pub, nmemi e.g. "1" ] },
	CNECTA { @ + 55 w 3 - [ pub, cnecta e.g. "715" ] },
	NECTADIV { @ + 56 w 5 - [ pub, nectadiv e.g. "99999" ] },
	CBSAPCI { @ + 57 w 1 - [ pub, cbsapci e.g. "N" ] },
	NECTAPCI { @ + 58 w 1 - [ pub, nectapci e.g. "N" ] },
	UA { @ + 59 w 5 - [ pub, ua e.g. "" ] },
	UATYPE { @ + 60 w 1 - [ pub, uatype e.g. "" ] },
	UR { @ + 61 w 1 - [ pub, ur e.g. "" ] },
	CD116 { @ + 62 w 2 - [ pub, cd116 e.g. "01" ] },
	CD118 { @ + 63 w 2 - [ pub, cd118 e.g. "" ] },
	CD119 { @ + 64 w 2 - [ pub, cd119 e.g. "" ] },
	CD120 { @ + 65 w 2 - [ pub, cd120 e.g. "" ] },
	CD121 { @ + 66 w 2 - [ pub, cd121 e.g. "" ] },
	SLDU18 { @ + 67 w 3 - [ pub, sldu18 e.g. "020" ] },
	SLDU22 { @ + 68 w 3 - [ pub, sldu22 e.g. "" ] },
	SLDU24 { @ + 69 w 3 - [ pub, sldu24 e.g. "" ] },
	SLDU26 { @ + 70 w 3 - [ pub, sldu26 e.g. "" ] },
	SLDU28 { @ + 71 w 3 - [ pub, sldu28 e.g. "" ] },
	SLDL18 { @ + 72 w 3 - [ pub, sldl18 e.g. "051" ] },
	SLDL22 { @ + 73 w 3 - [ pub, sldl22 e.g. "" ] },
	SLDL24 { @ + 74 w 3 - [ pub, sldl24 e.g. "" ] },
	SLDL26 { @ + 75 w 3 - [ pub, sldl26 e.g. "" ] },
	SLDL28 { @ + 76 w 3 - [ pub, sldl28 e.g. "" ] },
	VTD { @ + 77 w 6 - [ pub, vtd e.g. "443909" ] },
	VTDI { @ + 78 w 1 - [ pub, vtdi e.g. "A" ] },
	ZCTA { @ + 79 w 5 - [ pub, zcta e.g. "" ] },
	SDELM { @ + 80 w 5 - [ pub, sdelm e.g. "99999" ] },
	SDSEC { @ + 81 w 5 - [ pub, sdsec e.g. "99999" ] },
	SDUNI { @ + 82 w 5 - [ pub, sduni e.g. "01200" ] },
	PUMA { @ + 83 w 5 - [ pub, puma e.g. "" ] },

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
