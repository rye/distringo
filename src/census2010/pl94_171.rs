use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Table {
	P1,
	P2,
	P3,
	P4,
	H1,
}

pub use Table::{H1, P1, P2, P3, P4};

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum FileType {
	Tabular(usize),
	GeographicalHeader,
}

pub use FileType::Tabular;

macro_rules! generate_field_getter {
	($container_type:ty, $container_data_field:ident, $name:ident, [$vis:vis , $getter_name:ident #> $pty:ty]) => {
		#[allow(dead_code)]
		impl $container_type {
			#[must_use]
			$vis fn $getter_name(&self) -> $pty {
				self.$container_data_field[$name].parse::<$pty>().unwrap()
			}
		}
	};

	($container_type:ty, $container_data_field:ident, $name:ident, [$vis:vis , $getter_name:ident]) => {
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
	($container_type:ident, $container_data_field:ident, $($name:ident @ { $loc:expr } - $rest:tt),+) => {
		$(
			#[allow(dead_code)]
			const $name: core::ops::Range<usize> = $loc;

			generate_field_getter!($container_type, $container_data_field, $name, $rest);
		)+
	};
}

generate_fields!(
	GeographicalHeader, data,
	FILEID @ { 0..6 } - [pub, fileid],
	STUSAB @ { 6..8 } - [pub, stusab],
	SUMLEV @ { 8..11 } - [pub, sumlev],
	GEOCOMP @ { 11..13 } - [pub, geocomp],
	CHARITER @ { 13..16 } - [pub, chariter],
	CIFSN @ { 16..18 } - [pub, cifsn],
	LOGRECNO @ { 18..25 } - [pub, logrecno #> crate::LogicalRecordNumber],
	REGION @ { 25..26 } - [pub, region],
	DIVISION @ { 26..27 } - [pub, division],
	STATE @ { 27..29 } - [pub, state],
	COUNTY @ { 29..32 } - [pub, county],
	COUNTYCC @ { 32..34 } - [pub, countycc],
	COUNTYSC @ { 34..36 } - [pub, countysc],
	COUSUB @ { 36..41 } - [pub, cousub],
	COUSUBCC @ { 41..43 } - [pub, cousubcc],
	COUSUBSC @ { 43..45 } - [pub, cousubsc],
	PLACE @ { 45..50 } - [pub, place],
	PLACECC @ { 50..52 } - [pub, placecc],
	PLACESC @ { 52..54 } - [pub, placesc],
	TRACT @ { 54..60 } - [pub, tract],
	BLKGRP @ { 60..61 } - [pub, blkgrp],
	BLOCK @ { 61..65 } - [pub, block],
	IUC @ { 65..67 } - [pub, iuc],
	CONCIT @ { 67..72 } - [pub, concit],
	CONCITCC @ { 72..74 } - [pub, concitcc],
	CONCITSC @ { 74..76 } - [pub, concitsc],
	AIANHH @ { 76..80 } - [pub, aianhh],
	AIANHHFP @ { 80..85 } - [pub, aianhhfp],
	AIANHHCC @ { 85..87 } - [pub, aianhhcc],
	AIHHTLI @ { 87..88 } - [pub, aihhtli],
	AITSCE @ { 88..91 } - [pub, aitsce],
	AITS @ { 91..96 } - [pub, aits],
	AITSCC @ { 96..98 } - [pub, aitscc],
	TTRACT @ { 98..104 } - [pub, ttract],
	TBLKGRP @ { 104..105 } - [pub, tblkgrp],
	ANRC @ { 105..110 } - [pub, anrc],
	ANRCCC @ { 110..112 } - [pub, anrccc],
	CBSA @ { 112..117 } - [pub, cbsa],
	CBASC @ { 117..119 } - [pub, cbasc],
	METDIV @ { 119..124 } - [pub, metdiv],
	CSA @ { 124..127 } - [pub, csa],
	NECTA @ { 127..132 } - [pub, necta],
	NECTASC @ { 132..134 } - [pub, nectasc],
	NECTADIV @ { 134..139 } - [pub, nectadiv],
	CNECTA @ { 139..142 } - [pub, cnecta],
	CBSAPCI @ { 142..143 } - [pub, cbsapci],
	NECTAPCI @ { 143..144 } - [pub, nectapci],
	UA @ { 144..149 } - [pub, ua],
	UASC @ { 149..151 } - [pub, uasc],
	UATYPE @ { 151..152 } - [pub, uatype],
	UR @ { 152..153 } - [pub, ur],
	CD @ { 153..155 } - [pub, cd],
	SLDU @ { 155..158 } - [pub, sldu],
	SLDL @ { 158..161 } - [pub, sldl],
	VTD @ { 161..167 } - [pub, vtd],
	VTDI @ { 167..168 } - [pub, vtdi],
	RESERVE2 @ { 168..171 } - [, reserve2],
	ZCTA5 @ { 171..176 } - [pub, zcta5],
	SUBMCD @ { 176..181 } - [pub, submcd],
	SUBMCDCC @ { 181..183 } - [pub, submcdcc],
	SDELM @ { 183..188 } - [pub, sdelm],
	SDSEC @ { 188..193 } - [pub, sdsec],
	SDUNI @ { 193..198 } - [pub, sduni],
	AREALAND @ { 198..212 } - [pub, arealand],
	AREAWATR @ { 212..226 } - [pub, areawatr],
	NAME @ { 226..316 } - [pub, |name|],
	FUNCSTAT @ { 316..317 } - [pub, funcstat],
	GCUNI @ { 317..318 } - [pub, gcuni],
	POP100 @ { 318..327 } - [pub, pop100],
	HU100 @ { 327..336 } - [pub, hu100],
	INTPTLAT @ { 336..347 } - [pub, intptlat],
	INTPTLON @ { 347..359 } - [pub, intptlon],
	LSADC @ { 359..361 } - [pub, lsadc],
	PARTFLAG @ { 361..362 } - [pub, partflag],
	RESERVE3 @ { 362..368 } - [, reserve3],
	UGA @ { 368..373 } - [pub, uga],
	STATENS @ { 373..381 } - [pub, statens],
	COUNTYNS @ { 381..389 } - [pub, countyns],
	COUSUBNS @ { 389..397 } - [pub, cousubns],
	PLACENS @ { 397..405 } - [pub, placens],
	CONCITNS @ { 405..413 } - [pub, concitns],
	AIANHHNS @ { 413..421 } - [pub, aianhhns],
	AITSNS @ { 421..429 } - [pub, aitsns],
	ANRCNS @ { 429..437 } - [pub, anrcns],
	SUBMCDNS @ { 437..445 } - [pub, submcdns],
	CD113 @ { 445..447 } - [pub, cd113],
	CD114 @ { 447..449 } - [pub, cd114],
	CD115 @ { 449..451 } - [pub, cd115],
	SLDU2 @ { 451..454 } - [pub, sldu2],
	SLDU3 @ { 454..457 } - [pub, sldu3],
	SLDU4 @ { 457..460 } - [pub, sldu4],
	SLDL2 @ { 460..463 } - [pub, sldl2],
	SLDL3 @ { 463..466 } - [pub, sldl3],
	SLDL4 @ { 466..469 } - [pub, sldl4],
	AIANHHSC @ { 469..471 } - [pub, aianhhsc],
	CSASC @ { 471..473 } - [pub, csasc],
	CNECTASC @ { 473..475 } - [pub, cnectasc],
	MEMI @ { 475..476 } - [pub, memi],
	NMEMI @ { 476..477 } - [pub, nmemi],
	PUMA @ { 477..482 } - [pub, puma],
	RESERVED @ { 482..500 } - [, reserved]
);

pub struct GeographicalHeader {
	data: String,
}

impl GeographicalHeader {
	pub fn new(data: String) -> Self {
		Self { data }
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
