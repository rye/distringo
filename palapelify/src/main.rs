use std::collections::*;
use std::io::Read;

use geo_types::CoordNum;
use itertools::Itertools;

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

	let features: HashMap<&str, geo::LineString<f64>> = features
		.into_iter()
		.map(|(k, v)| {
			let geometry: &geojson::Geometry = v;
			use core::convert::TryInto;

			let geo_geometry: geo::Geometry<f64> = (geometry.value.clone())
				.try_into()
				.expect("failed to convert geometry");

			// TODO Replace clone() with iter constructing f64s directly from the reference.
			let ls: geo::LineString<f64> = match geo_geometry {
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
			};

			(k, ls)
		})
		.collect();

	let feature_count = features.len();

	println!(
		"Processing {} features ({} pairs)",
		feature_count,
		(feature_count * (feature_count - 1)) as f64 / 2.0
	);

	let pair_results: HashMap<(&str, &str), bool> = features
		.iter()
		.combinations(2)
		.map(|pair| {
			let name_a: &str = *pair[0].0;
			let name_b: &str = *pair[1].0;

			let overlaps: bool = {
				use geo::intersects::Intersects;

				pair[0].1.intersects(pair[1].1)
			};

			((name_a, name_b), overlaps)
		})
		.collect();

	for ((name_a, name_b), overlaps) in pair_results {
		if overlaps {
			println!("Features {} and {} overlap.", name_a, name_b);
		}
	}
}
