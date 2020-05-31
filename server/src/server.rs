pub mod routes;

pub fn server(
	settings: &config::Config,
) -> distringo::Result<warp::Server<impl warp::Filter<Extract = impl warp::Reply> + Clone>> {
	Ok(warp::serve(routes::routes(settings)?))
}

async fn handle_rejection(
	err: warp::Rejection,
) -> core::result::Result<impl warp::Reply, core::convert::Infallible> {
	if err.is_not_found() {
		Ok(warp::reply::with_status(
			warp::reply::html(include_str!("404.html")),
			http::StatusCode::NOT_FOUND,
		))
	} else {
		log::warn!("unhandled rejection: {:?}", err);
		Ok(warp::reply::with_status(
			warp::reply::html(include_str!("500.html")),
			http::StatusCode::INTERNAL_SERVER_ERROR,
		))
	}
}
