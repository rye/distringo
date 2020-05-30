pub mod api;

use warp::Filter;

pub fn routes(
	cfg: &config::Config,
) -> distringo::Result<warp::filters::BoxedFilter<(impl warp::Reply,)>> {
	let slash = warp::get()
		.and(warp::path::end())
		.and(warp::fs::file("./public/index.html"));

	let public_files = warp::get()
		.and(warp::fs::dir("./public/"))
		.and(warp::path::end());

	let files = slash.or(public_files);

	Ok(
		warp::any()
			.and(api::api(cfg)?.or(files))
			.with(warp::log("distringo"))
			.recover(crate::server::handle_rejection)
			.boxed(),
	)
}
