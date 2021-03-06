use std::collections::*;
use std::io::Read;

use itertools::Itertools;

fn feature_to_geometry(feature: &geojson::Feature) -> (&str, geo::Geometry<f64>) {
	use core::convert::TryInto;

	let feature_name: &str = feature.property("GEOID10").unwrap().as_str().unwrap();
	let geometry: &geojson::Geometry = (feature.geometry)
		.as_ref()
		.expect("geometry-less feature?!");

	let geometry: geo::Geometry<f64> = (geometry.value)
		.to_owned()
		.try_into()
		.expect("failed to convert geometry");

	(feature_name, geometry)
}

fn geometry_pair_to_adjacency_fragments<'x>(
	pair: ((&'x str, geo::Geometry<f64>), (&'x str, geo::Geometry<f64>)),
) -> Option<Vec<(&'x str, &'x str)>> {
	let name_a: &str = pair.0 .0;
	let name_b: &str = pair.1 .0;

	let overlaps: bool = {
		use geo::bounding_rect::BoundingRect;
		use geo::intersects::Intersects;

		let ls_a = &pair.0 .1;
		let ls_b = &pair.1 .1;

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

	if overlaps {
		Some(vec![(name_a, name_b), (name_b, name_a)])
	} else {
		None
	}
}

fn geojson_to_adjacency_map(geojson: &geojson::GeoJson) -> HashMap<&str, Vec<&str>> {
	let data: &geojson::FeatureCollection = match geojson {
		geojson::GeoJson::FeatureCollection(fc) => fc,
		_ => panic!("unsupported geojson type"),
	};

	let features: &Vec<geojson::Feature> = &data.features;

	let adjacency_map: HashMap<&str, Vec<&str>> = features
		.iter()
		.map(feature_to_geometry)
		.tuple_combinations()
		.filter_map(geometry_pair_to_adjacency_fragments)
		.flatten()
		.fold(HashMap::new(), |mut map, (name, neighbor)| {
			map.entry(name).or_insert(vec![]).push(neighbor);
			map
		});

	adjacency_map
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

	let adjacency_map = geojson_to_adjacency_map(&data);

	println!("{:#?}", adjacency_map);
}
