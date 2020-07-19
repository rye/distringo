pub use distringo::Result;

pub mod routes;

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

pub struct ExecutionPlan(config::Config);

impl From<config::Config> for ExecutionPlan {
	fn from(config: config::Config) -> Self {
		Self(config)
	}
}

impl ExecutionPlan {
	pub fn prepare(&self) -> Result<()> {
		// TODO(rye): Instead of doing nothing, perform a dry run of creating the
		// routes here so as to early-die if something is amiss.
		//
		// (Ideally, reduce the contract of execute())

		Ok(())
	}

	pub async fn execute(&mut self) -> Result<()> {
		use std::net::{IpAddr, SocketAddr};

		let socket = {
			use core::convert::TryInto;

			let config: &config::Config = &self.0;

			let host: IpAddr = config
				.get_str("server.host")?
				.parse()
				.map_err(|_| distringo::Error::InvalidServerHost)?;
			let port: u16 = config
				.get_int("server.port")?
				.try_into()
				.map_err(|_| distringo::Error::InvalidServerPort)?;

			SocketAddr::new(host, port)
		};

		warp::serve(routes::routes(&self.0)?).run(socket).await;

		Ok(())
	}
}
