pub mod api;

use warp::{filters::BoxedFilter, Filter, Reply};

pub fn routes(cfg: &config::Config) -> distringo::Result<BoxedFilter<(impl Reply,)>> {
	let slash = warp::get()
		.and(warp::path::end())
		.and(warp::fs::file("./public/index.html"));

	let public_files = warp::get()
		.and(warp::fs::dir("./public/"))
		.and(warp::path::end());

	let files = slash.or(public_files);

	let logging = warp::log("distringo");

	Ok(
		warp::any()
			.and(api::api(cfg)?.or(files))
			.with(logging)
			.recover(crate::server::handle_rejection)
			.boxed(),
	)
}
