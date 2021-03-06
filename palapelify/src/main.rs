use std::collections::*;
use std::io::Read;

use geo_types::CoordNum;
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

	let features: HashMap<&str, geo::LineString<f64>> =
		features.iter().map(feature_to_exterior).collect();

	let feature_count = features.len();
	let pairs = (feature_count * (feature_count - 1)) as f64 / 2.0;

	println!("Processing {} features ({} pairs)", feature_count, pairs);

	let mut ctr: f64 = 0.0;
	let t0 = std::time::Instant::now();

	let pair_results: HashMap<(&str, &str), bool> = features
		.iter()
		.combinations(2)
		.map(|pair| {
			let name_a: &str = *pair[0].0;
			let name_b: &str = *pair[1].0;

			let overlaps: bool = {
				use geo::bounding_rect::BoundingRect;
				use geo::intersects::Intersects;

				let ls_a: &geo::LineString<f64> = pair[0].1;
				let ls_b: &geo::LineString<f64> = pair[1].1;

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

			ctr += 1.0;

			let t = std::time::Instant::now();

			if ctr % 1000.0 == 0.0 {
				// prop done = (# done) / total
				let pct = ctr / pairs;

				// elapsed = t - t0
				let elapsed_s = t.duration_since(t0).as_secs_f64();

				// est_total = elapsed / (prop done)
				// remaining = est_total - elapsed_s
				let remaining_s = (elapsed_s) / (pct) - elapsed_s;

				println!(
					"Processed {} pairs of {} ({:.2}%; ETA: {:.0}s)",
					ctr,
					pairs,
					pct * 100.0,
					remaining_s
				);
			}

			((name_a, name_b), overlaps)
		})
		.collect();

	for ((name_a, name_b), overlaps) in pair_results {
		if overlaps {
			println!("Features {} and {} overlap.", name_a, name_b);
		}
	}
}
