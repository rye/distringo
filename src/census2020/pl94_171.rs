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

const RI_GEO2018_2020_STYLE_EXAMPLE: &str = "PLST|RI|040|00|00|000|00|0000001|0400000US44|44|1|1|44|01219835|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||1060563703|67858968|Rhode Island|Rhode Island|A||614053|271266|+41.8697678|-071.5786246|00||";

macro_rules! generate_field_getter {
	($container_type:ty, $container_data_field:ident, $name:ident, [ $vis:vis , $getter_name:ident #> $pty:ty ]) => {
		#[allow(dead_code)]
		impl $container_type {
			#[must_use]
			$vis fn $getter_name(&self) -> $pty {
				self.$container_data_field[$name].parse::<$pty>().unwrap()
			}
		}
	};

	($container_type:ty, $container_data_field:ident, $name:ident, [ $vis:vis , $getter_name:ident ]) => {
		#[allow(dead_code)]
		impl $container_type {
			#[must_use]
			$vis fn $getter_name(&self) -> &str {
				&self.$container_data_field[$name]
			}
		}
	};

	($container_type:ty, $container_data_field:ident, $name:ident, [ $vis:vis , | $getter_name:ident | ]) => {
		#[allow(dead_code)]
		impl $container_type {
			#[must_use]
			$vis fn $getter_name(&self) -> &str {
				&self.$container_data_field[$name].trim()
			}
		}
	};
}

macro_rules! generate_fields {
	($container_type:ident, $container_data_field:ident, $($name:ident @ + $loc:literal w $width:literal - $rest:tt),+) => {
		$(
			#[allow(dead_code)]
			const $name: usize = $loc;
		)+

		$(
			generate_field_getter!($container_type, $container_data_field, $name, $rest);
		)+
	};
}

generate_fields!(
	GeographicalHeader, data,

	// Record codes
	FILEID @ + 0 w 6 - [pub, fileid],
	STUSAB @ + 1 w 2 - [pub, stusab],
	SUMLEV @ + 2 w 3 - [pub, sumlev],
	// TODO(rye) 2020 +field: GEOVAR.
	GEOVAR @ + 3 w 2 - [pub, geovar],
	GEOCOMP @ + 4 w 2 - [pub, geocomp],
	CHARITER @ + 5 w 3 - [pub, chariter],
	CIFSN @ + 6 w 2 - [pub, cifsn],
	LOGRECNO @ + 7 w 7 - [pub, logrecno #> crate::LogicalRecordNumber],

	// Geographic Area Codes
	GEOID @ + 0 w 0 - [ pub, geoid ],
	GEOCODE @ + 0 w 0 - [ pub, geocode ],
	REGION @ + 0 w 0 - [ pub, region ],
	DIVISION @ + 0 w 0 - [ pub, division ],
	STATE @ + 0 w 0 - [ pub, state ],
	STATENS @ + 0 w 0 - [ pub, statens ],
	COUNTY @ + 0 w 0 - [ pub, county ],
	COUNTYCC @ + 0 w 0 - [ pub, countycc ],
	COUNTYNS @ + 0 w 0 - [ pub, countyns ],
	COUSUB @ + 0 w 0 - [ pub, cousub ],
	COUSUBCC @ + 0 w 0 - [ pub, cousubcc ],
	COUSUBNS @ + 0 w 0 - [ pub, cousubns ],
	SUBMCD @ + 0 w 0 - [ pub, submcd ],
	SUBMCDCC @ + 0 w 0 - [ pub, submcdcc ],
	SUBMCDNS @ + 0 w 0 - [ pub, submcdns ],
	ESTATE @ + 0 w 0 - [ pub, estate ],
	ESTATECC @ + 0 w 0 - [ pub, estatecc ],
	ESTATENS @ + 0 w 0 - [ pub, estatens ],
	CONCIT @ + 0 w 0 - [ pub, concit ],
	CONCITCC @ + 0 w 0 - [ pub, concitcc ],
	CONCITNS @ + 0 w 0 - [ pub, concitns ],
	PLACE @ + 0 w 0 - [ pub, place ],
	PLACECC @ + 0 w 0 - [ pub, placecc ],
	PLACENS @ + 0 w 0 - [ pub, placens ],
	TRACT @ + 0 w 0 - [ pub, tract ],
	BLKGRP @ + 0 w 0 - [ pub, blkgrp ],
	BLOCK @ + 0 w 0 - [ pub, block ],
	AIANHH @ + 0 w 0 - [ pub, aianhh ],
	AIHHTLI @ + 0 w 0 - [ pub, aihhtli ],
	AIANHHFP @ + 0 w 0 - [ pub, aianhhfp ],
	AIANHHCC @ + 0 w 0 - [ pub, aianhhcc ],
	AIANHHNS @ + 0 w 0 - [ pub, aianhhns ],
	AITS @ + 0 w 0 - [ pub, aits ],
	AITSFP @ + 0 w 0 - [ pub, aitsfp ],
	AITSCC @ + 0 w 0 - [ pub, aitscc ],
	AITSNS @ + 0 w 0 - [ pub, aitsns ],
	TTRACT @ + 0 w 0 - [ pub, ttract ],
	TBLKGRP @ + 0 w 0 - [ pub, tblkgrp ],
	ANRC @ + 0 w 0 - [ pub, anrc ],
	ANRCCC @ + 0 w 0 - [ pub, anrccc ],
	ANRCNS @ + 0 w 0 - [ pub, anrcns ],
	CBSA @ + 0 w 0 - [ pub, cbsa ],
	MEMI @ + 0 w 0 - [ pub, memi ],
	CSA @ + 0 w 0 - [ pub, csa ],
	METDIV @ + 0 w 0 - [ pub, metdiv ],
	NECTA @ + 0 w 0 - [ pub, necta ],
	NMEMI @ + 0 w 0 - [ pub, nmemi ],
	CNECTA @ + 0 w 0 - [ pub, cnecta ],
	NECTADIV @ + 0 w 0 - [ pub, nectadiv ],
	CBSAPCI @ + 0 w 0 - [ pub, cbsapci ],
	NECTAPCI @ + 0 w 0 - [ pub, nectapci ],
	UA @ + 0 w 0 - [ pub, ua ],
	UATYPE @ + 0 w 0 - [ pub, uatype ],
	UR @ + 0 w 0 - [ pub, ur ],
	CD116 @ + 0 w 0 - [ pub, cd116 ],
	CD118 @ + 0 w 0 - [ pub, cd118 ],
	CD119 @ + 0 w 0 - [ pub, cd119 ],
	CD120 @ + 0 w 0 - [ pub, cd120 ],
	CD121 @ + 0 w 0 - [ pub, cd121 ],
	SLDU18 @ + 0 w 0 - [ pub, sldu18 ],
	SLDU22 @ + 0 w 0 - [ pub, sldu22 ],
	SLDU24 @ + 0 w 0 - [ pub, sldu24 ],
	SLDU26 @ + 0 w 0 - [ pub, sldu26 ],
	SLDU28 @ + 0 w 0 - [ pub, sldu28 ],
	SLDL18 @ + 0 w 0 - [ pub, sldl18 ],
	SLDL22 @ + 0 w 0 - [ pub, sldl22 ],
	SLDL24 @ + 0 w 0 - [ pub, sldl24 ],
	SLDL26 @ + 0 w 0 - [ pub, sldl26 ],
	SLDL28 @ + 0 w 0 - [ pub, sldl28 ],
	VTD @ + 0 w 0 - [ pub, vtd ],
	VTDI @ + 0 w 0 - [ pub, vtdi ],
	ZCTA @ + 0 w 0 - [ pub, zcta ],
	SDELM @ + 0 w 0 - [ pub, sdelm ],
	SDSEC @ + 0 w 0 - [ pub, sdsec ],
	SDUNI @ + 0 w 0 - [ pub, sduni ],
	PUMA @ + 0 w 0 - [ pub, puma ],
	AREALAND @ + 0 w 0 - [ pub, arealand ],
	AREAWATR @ + 0 w 0 - [ pub, areawatr ],
	BASENAME @ + 0 w 0 - [ pub, basename ],
	NAME @ + 0 w 0 - [ pub, name ],
	FUNCSTAT @ + 0 w 0 - [ pub, funcstat ],
	GCUNI @ + 0 w 0 - [ pub, gcuni ],
	POP100 @ + 0 w 0 - [ pub, pop100 ],
	HU100 @ + 0 w 0 - [ pub, hu100 ],
	INTPTLAT @ + 0 w 0 - [ pub, intptlat ],
	INTPTLON @ + 0 w 0 - [ pub, intptlon ],
	LSADC @ + 0 w 0 - [ pub, lsadc ],
	PARTFLAG @ + 0 w 0 - [ pub, partflag ],
	UGA @ + 0 w 0 - [ pub, uga ]
);

pub struct GeographicalHeader {
	data: Vec<String>,
}

impl GeographicalHeader {
	pub fn new(data: String) -> Self {
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
