#[tokio::main]
async fn main() {
	pretty_env_logger::init();

	// GET / => (fs ./public/index.html)

	// GET /[path/to/files] => (fs ./public/[path/to/files])

	// Compose the routes together.
	let routes = warp::any()
		.map(warp::reply)
		.with(warp::log("uptown"));

	warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}
