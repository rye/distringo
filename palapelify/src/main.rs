use std::collections::*;
use std::io::Read;

use itertools::Itertools;

fn feature_to_geometry(feature: &geojson::Feature) -> (&str, geo::Geometry<f64>) {
	let feature_name: &str = feature.property("GEOID10").unwrap().as_str().unwrap();
	let geometry: &geojson::Geometry = (feature.geometry)
		.as_ref()
		.expect("geometry-less feature?!");

	use core::convert::TryInto;

	let geometry: geo::Geometry<f64> = (geometry.value)
		.to_owned()
		.try_into()
		.expect("failed to convert geometry");

	(feature_name, geometry)
}

const fn name_for_geometry(geometry: &geo::Geometry<f64>) -> &'static str {
	match geometry {
		geo::Geometry::MultiPolygon(_) => "MultiPolygon",
		geo::Geometry::Polygon(_) => "Polygon",
		geo::Geometry::GeometryCollection(_) => "GeometryCollection",
		geo::Geometry::Line(_) => "Line",
		geo::Geometry::Point(_) => "Point",
		geo::Geometry::LineString(_) => "LineString",
		geo::Geometry::MultiPoint(_) => "MultiPoint",
		geo::Geometry::MultiLineString(_) => "MultiLineString",
		geo::Geometry::Rect(_) => "Rect",
		geo::Geometry::Triangle(_) => "Triangle",
	}
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq)]
struct Timing<'a>(std::time::Duration, &'a str, &'a str, &'a str, &'a str);

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
		_ => panic!(""),
	};

	let features: &Vec<geojson::Feature> = &data.features;

	let mut ctr = 0;

	let mut timings: BTreeSet<Timing> = BTreeSet::new();

	let pair_results: HashMap<(&str, &str), bool> = features
		.iter()
		.map(feature_to_geometry)
		.tuple_combinations()
		.map(|(pair_a, pair_b)| {
			let name_a: &str = pair_a.0;
			let name_b: &str = pair_b.0;

			if ctr % 1000 == 0 {
				// println!("Processing pair #{}", ctr);
			}

			ctr += 1;

			let t0 = std::time::Instant::now();

			let overlaps: bool = {
				use geo::bounding_rect::BoundingRect;
				use geo::intersects::Intersects;

				let ls_a = &pair_a.1;
				let ls_b = &pair_b.1;

				let res = match (ls_a.bounding_rect(), ls_b.bounding_rect()) {
					// In nearly all cases, we should have bounding boxes, so check that they
					// overlap before doing the (more intense) operation of checking each segment
					// in a LineString for intersection.
					(Some(a_bb), Some(b_bb)) => a_bb.intersects(&b_bb) && ls_a.intersects(ls_b),
					// Fall back on simple LineString intersection checking if we couldn't figure
					// out bounding boxes (e.g. because of an empty LineString? this should be rare.)
					_ => ls_a.intersects(ls_b),
				};

				let t = std::time::Instant::now();

				let timing: Timing = Timing(
					t.duration_since(t0),
					name_a,
					name_b,
					name_for_geometry(ls_a),
					name_for_geometry(ls_b),
				);

				timings.insert(timing);

				res
			};

			((name_a, name_b), overlaps)
		})
		.collect();

	for ((name_a, name_b), overlaps) in pair_results {
		if overlaps {
			// println!("Features {} and {} overlap.", name_a, name_b);
		}
	}

	for timing in timings {
		println!(
			"{},{},{},{},{}ns",
			timing.1,
			timing.2,
			timing.3,
			timing.4,
			timing.0.as_nanos()
		);
	}
}
