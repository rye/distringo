use std::collections::*;
use std::io::Read;

use geo_types::CoordNum;
use itertools::Itertools;

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, PartialOrd)]
struct ApproxGeoDegree(i32);

impl From<ApproxGeoDegree> for f64 {
	fn from(a: ApproxGeoDegree) -> f64 {
		(a.0 as f64) / 1_000_000_f64
	}
}

impl num_traits::Num for ApproxGeoDegree {
	type FromStrRadixErr = std::num::ParseFloatError;

	fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
		Ok(str.parse::<f64>()?.into())
	}
}

impl core::ops::Mul for ApproxGeoDegree {
	type Output = Self;
	fn mul(self, other: Self) -> Self {
		(f64::from(self) * f64::from(other)).into()
	}
}

impl core::ops::Add for ApproxGeoDegree {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		(f64::from(self) + f64::from(other)).into()
	}
}

impl core::ops::Rem for ApproxGeoDegree {
	type Output = Self;
	fn rem(self, other: Self) -> Self {
		(f64::from(self) % f64::from(other)).into()
	}
}

impl core::ops::Div for ApproxGeoDegree {
	type Output = Self;
	fn div(self, other: Self) -> Self {
		(f64::from(self) / f64::from(other)).into()
	}
}

impl core::ops::Sub for ApproxGeoDegree {
	type Output = Self;
	fn sub(self, other: Self) -> Self {
		(f64::from(self) - f64::from(other)).into()
	}
}

impl num_traits::One for ApproxGeoDegree {
	fn one() -> Self {
		Self(1_i32)
	}
}

impl num_traits::Zero for ApproxGeoDegree {
	fn zero() -> Self {
		Self(0_i32)
	}

	fn is_zero(&self) -> bool {
		*self == ApproxGeoDegree::zero()
	}
}

impl num_traits::ToPrimitive for ApproxGeoDegree {
	fn to_i64(&self) -> Option<i64> {
		todo!()
	}
	fn to_u64(&self) -> Option<u64> {
		todo!()
	}
}

impl num_traits::cast::NumCast for ApproxGeoDegree {
	fn from<T>(pri: T) -> Option<Self> {
		todo!()
	}
}

impl From<f64> for ApproxGeoDegree {
	/// Convert a float with at most 6 decimal points of precision to this type.
	fn from(f: f64) -> Self {
		Self((f * 1_000_000.0_f64) as i32)
	}
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum OverlapType {
	None,
	SinglePoint,
	Segment,
	Full,
}

fn main() {
	let input_file: String = std::env::args().nth(1).expect("missing input file name");
	let output_file: String = std::env::args().nth(2).expect("missing output file name");

	let input_data: String = {
		let mut handle = std::fs::File::open(input_file).expect("failed to open file");
		let mut string: String = String::new();
		handle
			.read_to_string(&mut string)
			.expect("failed to read file to string");
		string
	};

	let data: geojson::GeoJson = input_data
		.parse::<geojson::GeoJson>()
		.expect("failed to parse input as geojson");

	let data: geojson::FeatureCollection = match data {
		geojson::GeoJson::FeatureCollection(fc) => fc,
		_ => unreachable!(),
	};

	let features: &Vec<geojson::Feature> = &data.features;

	let features: HashMap<&str, &geojson::Feature> = features
		.iter()
		.map(|feature| {
			(
				feature.property("GEOID10").unwrap().as_str().unwrap(),
				feature,
			)
		})
		.collect();

	let features: HashMap<&str, &geojson::Geometry> = features
		.into_iter()
		.map(|(k, v)| (k, (v.geometry).as_ref().expect("geometry-less feature?!")))
		.collect();

	let features: HashMap<&str, geo::LineString<ApproxGeoDegree>> = features
		.into_iter()
		.map(|(k, v)| {
			let geometry: &geojson::Geometry = v;
			use core::convert::TryInto;

			let geo_geometry: geo::Geometry<f64> = (geometry.value.clone())
				.try_into()
				.expect("failed to convert geometry");

			// TODO Replace clone() with iter constructing ApproxGeoDegrees directly from the reference.
			let ls: geo::LineString<ApproxGeoDegree> = match geo_geometry {
				geo::Geometry::Polygon(p) => p.exterior().clone(),
				geo::Geometry::MultiPolygon(ps) => geo::LineString(
					ps.iter()
						.map(|p| p.exterior().points_iter().map(Into::into))
						.flatten()
						.collect::<Vec<_>>(),
				),
				_ => panic!(
					"while processing {}: Geometry variant {:?} not yet supported",
					k, geo_geometry
				),
			}
			.into_iter()
			.map(|f: geo_types::Coordinate<f64>| geo_types::Coordinate {
				x: ApproxGeoDegree::from(f.x),
				y: ApproxGeoDegree::from(f.y),
			})
			.collect();

			(k, ls)
		})
		.collect();

	println!("Processing {} features", features.len());

	let pair_overlap_types: HashMap<(&str, &str), OverlapType> = features
		.iter()
		.combinations(2)
		.map(|pair| {
			let first_ls = pair[0].1;
			let last_ls = pair[1].1;
			let first_ls_pts: HashSet<geo_types::Point<ApproxGeoDegree>> =
				first_ls.clone().into_points().into_iter().collect();

			let last_ls_pts: HashSet<geo_types::Point<ApproxGeoDegree>> =
				last_ls.clone().into_points().into_iter().collect();

			let pts_count = first_ls_pts.len();

			let intersection = first_ls_pts.intersection(&last_ls_pts);

			let name_a: &str = *pair[0].0;
			let name_b: &str = *pair[1].0;

			let overlap_type: OverlapType = match intersection.count() {
				0 => OverlapType::None,
				1 => OverlapType::SinglePoint,
				c if c == pts_count => OverlapType::Full,
				_ => OverlapType::Segment,
			};

			((name_a, name_b), overlap_type)
		})
		.collect();

	for ((feature_a, feature_b), t) in pair_overlap_types {
		if t != OverlapType::None {}
	}
}
