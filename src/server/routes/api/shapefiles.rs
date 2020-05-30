use core::convert::TryFrom;
use geojson::GeoJson;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShapefileType {
	#[serde(alias = "tabblock")]
	TabularBlock,
}

#[derive(Debug)]
pub struct Shapefile {
	ty: ShapefileType,
	contents: GeoJson,
	data: hyper::body::Bytes,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ShapefileConfiguration {
	#[serde(rename = "type")]
	ty: ShapefileType,
	file: String,
}

impl Shapefile {
	pub fn from_file<P: AsRef<Path>>(ty: ShapefileType, path: P) -> distringo::Result<Self> {
		let contents = std::fs::read_to_string(path)?.parse::<GeoJson>()?;
		let data = contents.to_string().into();

		Ok(Self { ty, contents, data })
	}
}

impl TryFrom<ShapefileConfiguration> for Shapefile {
	type Error = distringo::Error;

	fn try_from(sc: ShapefileConfiguration) -> distringo::Result<Self> {
		Self::from_file(sc.ty, sc.file)
	}
}

pub fn index(shapefiles: &Arc<std::collections::HashMap<String, Shapefile>>) -> impl warp::Reply {
	warp::reply::json(&shapefiles.keys().collect::<Vec<&String>>())
}

pub fn show(
	shapefiles: &Arc<std::collections::HashMap<String, Shapefile>>,
	id: &String,
) -> hyper::Response<hyper::body::Bytes> {
	if let Some(shapefile) = shapefiles.get(id) {
		let t0: std::time::Instant = std::time::Instant::now();

		// This line _does not_ "clone" the entire data.
		//
		// `shapefile.data` is a reference-counted thing which is pre-filled at
		// startup time before this function can be called.  This means we can
		// pretty easily get ahold of an owned `Bytes` structure surrounding the
		// thing.
		let data = shapefile.data.clone();

		let t1: std::time::Instant = std::time::Instant::now();

		let response = {
			http::response::Builder::new()
				.status(hyper::StatusCode::OK)
				.header(hyper::header::CONTENT_TYPE, "application/vnd.geo+json")
				.header(hyper::header::CACHE_CONTROL, "public")
				// TODO(rye): Clean up error path
				.body(data)
				.unwrap()
		};

		let t2: std::time::Instant = std::time::Instant::now();

		log::trace!(
			"Prepared data in {}ns, response in {}ns",
			t1.duration_since(t0).as_nanos(),
			t2.duration_since(t1).as_nanos()
		);

		response
	} else {
		log::debug!("{:?}", shapefiles);

		let response = {
			http::response::Builder::new()
				.status(hyper::StatusCode::NOT_FOUND)
				.body("{}".to_string().into())
				.unwrap()
		};

		response
	}
}
