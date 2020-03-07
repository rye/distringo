use hyper::StatusCode;
use log::warn;

use warp::{Filter, Rejection, Reply};

#[tokio::main]
async fn main() {
	if std::env::var("RUST_LOG").ok().is_none() {
		std::env::set_var("RUST_LOG", "info");
	}

	pretty_env_logger::init();

	let _data = uptown::parser::pl94_171::Dataset::load(
		uptown::schema::GeographicalHeaderSchema::Census2010,
		"./in2010.pl.prd.packinglist.txt",
		"./ingeo2010.pl",
		vec!["./in000012010.pl", "./in000022010.pl"],
	);

	// println!("{:#?}", data);

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
		.and(warp::get().and(slash.or(public_files)))
		.with(warp::log("uptown"))
		.recover(handle_rejection);

	warp::serve(routes)
		.run(([0, 0, 0, 0, 0, 0, 0, 0], 2020))
		.await
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
