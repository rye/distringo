use config::Config;
use hyper::{Response, StatusCode};
use log::warn;
use std::collections::HashMap;
use std::path::PathBuf;
use uptown::parser::pl94_171::Dataset;

use std::net::IpAddr;
use std::net::SocketAddr;

use warp::{Filter, Rejection, Reply};

fn api() -> impl warp::Filter<Extract = impl Reply, Error = Rejection> + Clone {
	// GET /api/data/tabblock/<year>/<fips_state>/<fips_county>
	let tabblock = warp::get()
		.and(warp::path!("tabblock" / u16 / u16 / u16))
		.map(|year, fips_state, fips_county| {
			Response::builder()
				.body(format!(
					"You requested data for year {} for {}{}",
					year, fips_state, fips_county
				))
				.unwrap()
		});

	// GET /api/data/pl94_171/<year>/<stusab>
	let pl94_171 = warp::get()
		.and(warp::path!("pl94_171" / u16 / String))
		.map(|year, stusab| {
			Response::builder()
				.body(format!(
					"You requested PL94-171 data for year {} in state {}",
					year, stusab
				))
				.unwrap()
		});

	warp::path!("api" / "data").and(tabblock.or(pl94_171))
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
	let routes = warp::any()
		.and(warp::get().and(slash.or(public_files)).or(api()))
		.with(warp::log("uptown"))
		.recover(handle_rejection);

	routes
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

	let datasets: HashMap<String, Box<Dataset>> = {
		let datasets = settings.get_table("datasets")?;

		datasets
			.iter()
			.map(
				|(name, value)| -> uptown::error::Result<(String, Box<Dataset>)> {
					let value: HashMap<String, config::Value> = value.clone().into_table()?;

					let packing_list: PathBuf = value
						.get("packing_list")
						.ok_or(uptown::error::Error::MissingPackingList)?
						.clone()
						.into_str()?
						.into();

					let dataset: Box<Dataset> = Box::new(Dataset::load(packing_list)?);

					Ok((name.to_string(), dataset))
				},
			)
			.filter_map(Result::ok)
			.collect()
	};

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
