#[tokio::main]
async fn main() {
	pretty_env_logger::init();

	// GET / => (fs ./public/index.html)

	// GET /[path/to/files] => (fs ./public/[path/to/files])

	// Compose the routes together.
}
