use config::Config;
use hyper::{Response, StatusCode};
use log::warn;

use std::net::IpAddr;
use std::net::SocketAddr;

use warp::{Filter, Rejection, Reply};

fn shapefiles() -> warp::filters::BoxedFilter<(impl Reply,)> {
	// GET .../shapefile/<id>
	warp::get()
		.and(warp::path!("shapefile" / String))
		.map(|shapefile_name| {
			Response::builder()
				.body(format!("shp {}", shapefile_name))
				.unwrap()
		})
		.boxed()
}

fn datasets() -> warp::filters::BoxedFilter<(impl Reply,)> {
	// GET .../dataset/<id>
	warp::get()
		.and(warp::path!("dataset" / String))
		.map(|dataset_name| {
			Response::builder()
				.body(format!("ds {}", dataset_name))
				.unwrap()
		})
		.boxed()
}

fn api() -> warp::filters::BoxedFilter<(impl Reply,)> {
	warp::path!("api" / ..)
		.and(shapefiles().or(datasets()))
		.boxed()
}

fn routes() -> impl warp::Filter<Extract = impl Reply> + Clone {
	// GET / => (fs ./public/index.html)
	let slash = warp::get()
		.and(warp::path::end())
		.and(warp::fs::file("./public/index.html"));

	// GET /[path/to/files] => (fs ./public/[path/to/files])
	let public_files = warp::get()
		.and(warp::fs::dir("./public/"))
		.and(warp::path::end());

	// Compose the routes together.
	warp::any()
		.and(warp::get().and(slash.or(public_files)).or(api()))
		.with(warp::log("uptown"))
		.recover(handle_rejection)
}

#[tokio::main]
async fn main() -> uptown::error::Result<()> {
	if std::env::var("RUST_LOG").ok().is_none() {
		std::env::set_var("RUST_LOG", "info");
	}

	pretty_env_logger::init();

	let mut settings = Config::default();

	settings.set_default("server.host", "::")?;
	settings.set_default("server.port", 2020)?;

	settings.merge(config::Environment::with_prefix("UPTOWN"))?;

	settings.merge(config::File::with_name("config"))?;

	let socket = {
		use core::convert::TryInto;

		let host: IpAddr = settings
			.get_str("server.host")?
			.parse()
			.map_err(|_| uptown::error::Error::InvalidServerHost)?;
		let port: u16 = settings
			.get_int("server.port")?
			.try_into()
			.map_err(|_| uptown::error::Error::InvalidServerPort)?;

		SocketAddr::new(host, port)
	};

	warp::serve(routes()).run(socket).await;

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
