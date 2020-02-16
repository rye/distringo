use hyper::StatusCode;
use log::warn;
use warp::{Filter, Rejection, Reply};

#[tokio::main]
async fn main() {
	if std::env::var("RUST_LOG").ok().is_none() {
		std::env::set_var("RUST_LOG", "info");
	}

	pretty_env_logger::init();

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

	warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}

async fn handle_rejection(err: Rejection) -> Result<impl Reply, core::convert::Infallible> {
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
