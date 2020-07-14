use warp::{filters::BoxedFilter, fs, path, Filter, Reply};

pub mod api;

pub fn routes(cfg: &config::Config) -> distringo::Result<BoxedFilter<(impl Reply,)>> {
	let slash = warp::get()
		.and(path::end())
		.and(fs::file("./public/index.html"));

	let public_files = warp::get().and(fs::dir("./public/")).and(path::end());

	let file_routes = slash.or(public_files);

	let api_routes = api::api(cfg)?;

	let root = api_routes
		.or(file_routes)
		.with(warp::log("distringo"))
		.recover(super::handle_rejection)
		.boxed();

	Ok(root)
}
