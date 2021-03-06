use std::collections::*;
use std::io::Read;

use itertools::Itertools;

fn feature_to_exterior(feature: &geojson::Feature) -> (&str, geo::LineString<f64>) {
	let feature_name: &str = feature.property("GEOID10").unwrap().as_str().unwrap();
	let geometry: &geojson::Geometry = (feature.geometry)
		.as_ref()
		.expect("geometry-less feature?!");

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
			feature_name, geo_geometry
		),
	};

	(feature_name, ls)
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

	let pair_results: HashMap<(&str, &str), bool> = features
		.iter()
		.map(feature_to_exterior)
		.combinations(2)
		.map(|pair| {
			let name_a: &str = pair[0].0;
			let name_b: &str = pair[1].0;

			let overlaps: bool = {
				use geo::bounding_rect::BoundingRect;
				use geo::intersects::Intersects;

				let ls_a: &geo::LineString<f64> = &pair[0].1;
				let ls_b: &geo::LineString<f64> = &pair[1].1;

				match (ls_a.bounding_rect(), ls_b.bounding_rect()) {
					// In nearly all cases, we should have bounding boxes, so check that they
					// overlap before doing the (more intense) operation of checking each segment
					// in a LineString for intersection.
					(Some(a_bb), Some(b_bb)) => a_bb.intersects(&b_bb) && ls_a.intersects(ls_b),
					// Fall back on simple LineString intersection checking if we couldn't figure
					// out bounding boxes (e.g. because of an empty LineString? this should be rare.)
					_ => ls_a.intersects(ls_b),
				}
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
