use core::convert::TryFrom;

use std::path::Path;

use geojson::GeoJson;
use hyper::body::Body;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShapefileType {
	#[serde(alias = "tabblock")]
	TabularBlock,
}

pub struct ByteChunkStream<'buffer> {
	iterator: std::slice::Chunks<'buffer, u8>,
}

impl<'buffer> ByteChunkStream<'buffer> {
	fn new(buf: &'buffer str, chunk_size: usize) -> Self {
		Self {
			iterator: buf.as_bytes().chunks(chunk_size),
		}
	}
}

use std::{
	pin::Pin,
	task::{Context, Poll},
};

impl<'buffer> futures::Stream for ByteChunkStream<'buffer> {
	type Item = Result<&'buffer [u8], std::convert::Infallible>;

	fn poll_next(self: Pin<&mut Self>, _ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let next: Option<Result<&'buffer [u8], _>> = self.get_mut().iterator.next().map(Ok);
		Poll::Ready(next)
	}
}

#[derive(Debug)]
pub struct Shapefile {
	ty: ShapefileType,
	contents: GeoJson,
	data: String,
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

		// TODO(rye): Avoid re-allocating as a String by having a more "streamable" result.
		let data = contents.to_string();

		Ok(Self { ty, contents, data })
	}
}

impl TryFrom<ShapefileConfiguration> for Shapefile {
	type Error = distringo::Error;

	fn try_from(sc: ShapefileConfiguration) -> distringo::Result<Self> {
		Self::from_file(sc.ty, sc.file)
	}
}

pub fn index(shapefiles: &std::collections::HashMap<String, Shapefile>) -> impl warp::Reply {
	warp::reply::json(&shapefiles.keys().collect::<Vec<&String>>())
}

pub fn show(
	shapefiles: &'static std::collections::HashMap<String, Shapefile>,
	id: &str,
) -> hyper::Response<Body> {
	if let Some(shapefile) = shapefiles.get(id) {
		let data: &str = &shapefile.data;

		http::response::Builder::new()
			.status(hyper::StatusCode::OK)
			.header(hyper::header::CONTENT_TYPE, "application/vnd.geo+json")
			.header(hyper::header::CACHE_CONTROL, "public")
			// TODO(rye): Clean up error path
			.body(Body::wrap_stream(ByteChunkStream::new(data, 4096)))
			.unwrap()
	} else {
		log::debug!("{:?}", shapefiles);

		http::response::Builder::new()
			.status(hyper::StatusCode::NOT_FOUND)
			.body("{}".into())
			.unwrap()
	}
}

#[cfg(test)]
mod tests {
	use super::{Shapefile, ShapefileType};
	#[cfg(test)]
	mod show {
		use super::{Shapefile, ShapefileType};
		use geojson::{GeoJson, Geometry, Value::Point};
		use once_cell::sync::OnceCell;
		use std::collections::HashMap;

		static CACHE: OnceCell<HashMap<String, Shapefile>> = OnceCell::new();

		fn generate_id_and_shapefiles() -> (String, &'static HashMap<String, Shapefile>) {
			let contents = GeoJson::Geometry(Geometry::new(Point(vec![0.0_f64, 0.0_f64])));
			let shapefile = Shapefile {
				ty: ShapefileType::TabularBlock,
				data: contents.to_string(),
				contents,
			};

			let id = "id".to_string();
			let map: &'static HashMap<String, Shapefile> = {
				if CACHE.get().is_none() {
					let mut map = HashMap::new();
					map.insert(id.clone(), shapefile);
					CACHE
						.set(map)
						.unwrap_or_else(|_| eprintln!("cache already initialized"));
				}

				CACHE.get().unwrap()
			};

			(id, map)
		}

		#[test]
		fn found_returns_200_ok() {
			let (id, map) = generate_id_and_shapefiles();
			let response = super::super::show(map, &id);
			assert_eq!(response.status(), hyper::StatusCode::OK);
		}

		#[test]
		fn found_returns_correct_headers() {
			let (id, map) = generate_id_and_shapefiles();
			let response = super::super::show(map, &id);
			assert_eq!(
				response
					.headers()
					.get(hyper::header::CONTENT_TYPE)
					.expect("missing content-type header"),
				"application/vnd.geo+json"
			);
			assert_eq!(
				response
					.headers()
					.get(hyper::header::CACHE_CONTROL)
					.expect("missing cache-control header"),
				"public"
			);
		}

		macro_rules! assert_response_body_eq {
			($response:ident, $value:literal) => {
				use hyper::body::Bytes;
				use tokio_stream::StreamExt;
				assert_eq!(
					$response
						.into_body()
						.filter_map(|r| r.ok())
						.fold(Bytes::new(), |acc, new| -> Bytes {
							[acc, new.clone()].concat().into()
						})
						.await,
					Bytes::from($value)
				);
			};
		}

		#[tokio::test]
		async fn found_returns_correct_body() {
			let (id, map) = generate_id_and_shapefiles();
			let response = super::super::show(map, &id);
			assert_response_body_eq!(response, "{\"coordinates\":[0.0,0.0],\"type\":\"Point\"}");
		}

		#[test]
		fn not_found_returns_404() {
			let (_id, map) = generate_id_and_shapefiles();

			let response = super::super::show(map, &"<some unknown id>".to_string());

			assert_eq!(response.status(), hyper::StatusCode::NOT_FOUND);
		}
	}
}
