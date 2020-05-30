pub mod api;

use warp::{filters::BoxedFilter, fs, path, Filter, Reply};

pub fn routes(cfg: &config::Config) -> distringo::Result<BoxedFilter<(impl Reply,)>> {
	let slash = warp::get()
		.and(path::end())
		.and(fs::file("./public/index.html"));

	let public_files = warp::get().and(fs::dir("./public/")).and(path::end());

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
