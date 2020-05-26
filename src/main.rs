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
				use std::sync::Arc;

				pub fn index(
					shapefiles: &Arc<std::collections::HashMap<String, config::Value>>,
				) -> impl warp::Reply {
					warp::reply::json(&shapefiles.keys().collect::<Vec<&String>>())
				}

				// TODO(rye): Change signature: shapefile: &Shapefile -> http::Response<String>
				pub fn show(
					shapefiles: &Arc<std::collections::HashMap<String, config::Value>>,
					id: &String,
				) -> hyper::Response<String> {
					if let Some(config) = shapefiles.get(id) {
						// let table = config.into_table()?;

						// TODO(rye): load the data
						// TODO(rye): just keep an in-memory stash of the shapefiles and refute booting if files in config don't exist? (better)
						let data: String = "{}".to_string();

						let response = {
							http::response::Builder::new()
								.status(hyper::StatusCode::OK)
								.header("Content-Type", "application/vnd.geo+json")
								// TODO(rye): Clean up error path
								.body(data)
								.unwrap()
						};

						response
					} else {
						log::debug!("{:?}", shapefiles);

						let response = {
							http::response::Builder::new()
								.status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
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
				let shapefiles: Arc<std::collections::HashMap<String, config::Value>> =
					Arc::new(cfg.get_table("datasets")?);

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
