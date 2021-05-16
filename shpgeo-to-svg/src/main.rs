use core::convert::{TryFrom, TryInto};
use std::collections::*;

use geojson::{quick_collection, GeoJson};

use geo_svg::ToSvg;

fn main() {
	let args: Vec<String> = std::env::args().collect();

	assert!(
		args.len() >= 4,
		"Need at least a shapefile, a key to use for lookup, and a single geoid to render"
	);

	let shapefile = &args[1];
	let key = &args[2];

	let geoids = &args[3..];

	eprintln!("Printing geoids {:?} from shapefile {}", geoids, shapefile);

	let file_data: String =
		std::fs::read_to_string(shapefile).expect("failed to read specified shapefile to string");

	let geojson = file_data.parse::<GeoJson>().unwrap();

	if let geojson::GeoJson::FeatureCollection(fc) = geojson {
		// Load the geojson file to a GeometryCollection

		let n_geometries = fc.features.len();

		// Destructure the geometries into a BTreeMap<geoid, geometry>

		let all_geometries: BTreeMap<String, geo_types::Geometry<f64>> = fc
			.features
			.into_iter()
			.enumerate()
			.map(|(idx, feature): (usize, _)| {
				let geoid = feature
					.properties
					.expect("features should have properties")
					.get(key)
					.expect("missing property for geoid key")
					.as_str()
					.expect("should be able to coerce geoid key to str")
					.to_string();

				let geometry: geo_types::Geometry<f64> = feature
					.geometry
					.expect("geometry-less feature")
					.try_into()
					.expect("failed to convert geometry");

				(geoid, geometry)
			})
			.collect();
		let matching_geometries: geo_types::GeometryCollection<f64> = if geoids
			.iter()
			.all(|geoid| all_geometries.contains_key(geoid))
		{
			eprintln!(
				"Exact match on geoids {:?}; rendering the collection without searching",
				geoids
			);

			geoids
				.iter()
				.map(|geoid| {
					all_geometries
						.get(geoid)
						.expect("geoid now missing?")
						.to_owned()
				})
				.collect()
		} else {
			eprintln!(
				"At least one non-exact match on geoids {:?}; performing range concats",
				geoids
			);

			todo!()
		};

		eprintln!("Matching geometries: {:?}", matching_geometries);

		let stroke_width = "0.00001";
		let margin = "0.0001";

		let svg: geo_svg::Svg = matching_geometries.to_svg().with_margin(0.01_f32);

		println!("{}", svg);
	} else {
		panic!("not a shapefile")
	}
}
