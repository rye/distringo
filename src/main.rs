use config::Config;
use hyper::StatusCode;
use log::warn;

use std::net::IpAddr;
use std::net::SocketAddr;

use warp::{Filter, Rejection, Reply};

pub mod routes {
	pub mod api {
		pub mod v0 {
			use std::sync::Arc;
			use warp::Filter;

			pub mod shapefiles {
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
					data: GeoJson,
				}

				#[derive(serde::Serialize, serde::Deserialize)]
				pub struct ShapefileConfiguration {
					#[serde(rename = "type")]
					ty: ShapefileType,
					file: String,
				}

				impl Shapefile {
					pub fn from_file<P: AsRef<Path>>(ty: ShapefileType, path: P) -> distringo::Result<Self> {
						let data = std::fs::read_to_string(path)?.parse::<GeoJson>()?;

						Ok(Self { ty, data })
					}
				}

				impl TryFrom<ShapefileConfiguration> for Shapefile {
					type Error = distringo::Error;

					fn try_from(sc: ShapefileConfiguration) -> distringo::Result<Self> {
						Self::from_file(sc.ty, sc.file)
					}
				}

				pub fn index(
					shapefiles: &Arc<std::collections::HashMap<String, Shapefile>>,
				) -> impl warp::Reply {
					warp::reply::json(&shapefiles.keys().collect::<Vec<&String>>())
				}

				// TODO(rye): Change signature: shapefile: &Shapefile -> http::Response<String>
				pub fn show(
					shapefiles: &Arc<std::collections::HashMap<String, Shapefile>>,
					id: &String,
				) -> hyper::Response<String> {
					if let Some(shapefile) = shapefiles.get(id) {
						let t0: std::time::Instant = std::time::Instant::now();

						// TODO(rye): Figure out how to send this more efficiently; this takes about 3 seconds.
						let data: String = shapefile.data.to_string();

						let t1: std::time::Instant = std::time::Instant::now();

						let response = {
							http::response::Builder::new()
								.status(hyper::StatusCode::OK)
								.header(hyper::header::CONTENT_TYPE, "application/vnd.geo+json")
								.header(hyper::header::CACHE_CONTROL, "public ")
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
								.body("{}".to_string())
								.unwrap()
						};

						response
					}
				}
			}

			pub fn shapefiles(
				cfg: &config::Config,
			) -> distringo::Result<warp::filters::BoxedFilter<(impl warp::Reply,)>> {
				let shapefiles: std::collections::HashMap<String, config::Value> =
					cfg.get_table("shapefiles")?;

				use core::convert::TryInto;
				use shapefiles::{Shapefile, ShapefileConfiguration};

				let shapefiles: std::collections::HashMap<String, Shapefile> = shapefiles
					.iter()
					.filter_map(
						|(id, value): (&String, &config::Value)| -> Option<(String, Shapefile)> {
							// TODO(rye): avoid clone by iterating over keys and using remove?
							let value: config::Value = value.clone();
							// TODO(rye): handle error a bit better
							let config: ShapefileConfiguration = value.try_into().expect("invalid configuration");
							let shapefile: distringo::Result<Shapefile> = config.try_into();
							if let Ok(shapefile) = shapefile {
								Some((id.to_string(), shapefile))
							} else {
								log::warn!("Error parsing shapefile {}: {:?}", id, shapefile);
								None
							}
						},
					)
					.collect();

				let shapefiles: Arc<std::collections::HashMap<String, Shapefile>> = Arc::new(shapefiles);

				// GET /api/v0/shapefiles
				let shapefiles_index = {
					let shapefiles = shapefiles.clone();
					warp::get()
						.and(warp::path::end())
						.map(move || shapefiles::index(&shapefiles))
				};
				// GET /api/v0/shapefiles/:id
				let shapefiles_show = {
					let shapefiles = shapefiles.clone();
					warp::get()
						.and(warp::path!(String))
						.map(move |id: String| shapefiles::show(&shapefiles, &id))
						.with(warp::compression::gzip())
				};
				// ... /api/v0/shapefiles/...
				let shapefiles = warp::any()
					.and(warp::path!("shapefiles" / ..))
					.and(shapefiles_index.or(shapefiles_show))
					.boxed();

				Ok(shapefiles)
			}
		}
	}
}

fn api_v0(cfg: &Config) -> distringo::Result<warp::filters::BoxedFilter<(impl warp::Reply,)>> {
	let shapefiles = routes::api::v0::shapefiles(cfg)?;

	let api = warp::path("api");
	let api_v0 = api.and(warp::path("v0"));

	let gets = shapefiles;

	Ok(warp::any().and(api_v0).and(gets).boxed())
}

fn routes(cfg: &Config) -> distringo::Result<warp::filters::BoxedFilter<(impl warp::Reply,)>> {
	let slash = warp::get()
		.and(warp::path::end())
		.and(warp::fs::file("./public/index.html"));

	let public_files = warp::get()
		.and(warp::fs::dir("./public/"))
		.and(warp::path::end());

	let files = slash.or(public_files);

	Ok(
		warp::any()
			.and(api_v0(cfg)?.or(files))
			.with(warp::log("distringo"))
			.recover(handle_rejection)
			.boxed(),
	)
}

#[tokio::main]
async fn main() -> distringo::Result<()> {
	if std::env::var("DISTRINGO_LOG").ok().is_none() {
		std::env::set_var("DISTRINGO_LOG", "info");
	}

	pretty_env_logger::init_custom_env("DISTRINGO_LOG");

	let mut settings = Config::default();

	settings.set_default("server.host", "::")?;
	settings.set_default("server.port", 2020)?;

	settings.merge(config::Environment::with_prefix("DISTRINGO"))?;

	settings.merge(config::File::with_name("config"))?;

	let socket = {
		use core::convert::TryInto;

		let host: IpAddr = settings
			.get_str("server.host")?
			.parse()
			.map_err(|_| distringo::Error::InvalidServerHost)?;
		let port: u16 = settings
			.get_int("server.port")?
			.try_into()
			.map_err(|_| distringo::Error::InvalidServerPort)?;

		SocketAddr::new(host, port)
	};

	warp::serve(routes(&settings)?).run(socket).await;

	Ok(())
}

async fn handle_rejection(
	err: Rejection,
) -> core::result::Result<impl Reply, core::convert::Infallible> {
	if err.is_not_found() {
		Ok(warp::reply::with_status(
			warp::reply::html(include_str!("404.html")),
			StatusCode::NOT_FOUND,
		))
	} else {
		warn!("unhandled rejection: {:?}", err);
		Ok(warp::reply::with_status(
			warp::reply::html(include_str!("500.html")),
			StatusCode::INTERNAL_SERVER_ERROR,
		))
	}
}
