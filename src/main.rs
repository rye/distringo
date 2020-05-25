use config::Config;
use hyper::StatusCode;
use log::warn;
use std::sync::Arc;

use std::net::IpAddr;
use std::net::SocketAddr;

use warp::{Filter, Rejection, Reply};

fn api_v0(cfg: &Config) -> distringo::Result<warp::filters::BoxedFilter<(impl warp::Reply,)>> {
	let shapefiles: Arc<std::collections::HashMap<String, config::Value>> =
		Arc::new(cfg.get_table("datasets")?);

	// GET /api/v0/shapefiles
	let shapefiles_index = {
		let shapefiles = shapefiles.clone();
		warp::get()
			.and(warp::path::end())
			.map(move || warp::reply::json(&shapefiles.keys().collect::<Vec<&String>>()))
	};
	// GET /api/v0/shapefiles/:id
	let shapefiles_show = {
		let shapefiles = shapefiles.clone();
		warp::get()
			.and(warp::path!(String))
			.map(move |id: String| format!("shapefiles#show (id={:?}): {:?}", id, shapefiles.get(&id)))
	};
	// ... /api/v0/shapefiles/...
	let shapefiles = warp::any()
		.and(warp::path!("shapefiles" / ..))
		.and(shapefiles_index.or(shapefiles_show));

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
