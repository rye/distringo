use config::Config;
use hyper::StatusCode;
use log::warn;

use std::net::IpAddr;
use std::net::SocketAddr;

use warp::{Filter, Rejection, Reply};

fn api_v0() -> warp::filters::BoxedFilter<(impl warp::Reply,)> {
	let shapefiles_index = warp::get()
		.and(warp::path::end())
		.map(|| format!("shapefiles#index"));
	let shapefiles_show = warp::get()
		.and(warp::path!(String))
		.map(|id: String| format!("shapefiles#show (id={:?})", id));
	let shapefiles = warp::any()
		.and(warp::path!("shapefiles" / ..))
		.and(shapefiles_index.or(shapefiles_show));

	let sessions_index = warp::get()
		.and(warp::path::end())
		.map(|| format!("sessions#index"));
	let sessions_show = warp::get()
		.and(warp::path!(String))
		.map(|id: String| format!("sessions#show (id={:?})", id));
	let sessions = warp::any()
		.and(warp::path!("sessions" / ..))
		.and(sessions_index.or(sessions_show));

	let api = warp::path("api");
	let api_v0 = api.and(warp::path("v0"));

	let gets = shapefiles.or(sessions);

	warp::any().and(api_v0).and(gets).boxed()
}

fn routes() -> warp::filters::BoxedFilter<(impl warp::Reply,)> {
	let slash = warp::get()
		.and(warp::path::end())
		.and(warp::fs::file("./public/index.html"));

	let public_files = warp::get()
		.and(warp::fs::dir("./public/"))
		.and(warp::path::end());

	let files = slash.or(public_files);

	warp::any()
		.and(api_v0().or(files))
		.with(warp::log("distringo"))
		.recover(handle_rejection)
		.boxed()
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
