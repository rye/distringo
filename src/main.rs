use config::Config;

use std::net::IpAddr;
use std::net::SocketAddr;

pub mod server;

#[tokio::main]
async fn main() -> distringo::Result<()> {
	if std::env::var("DISTRINGO_LOG").ok().is_none() {
		std::env::set_var("DISTRINGO_LOG", "info");
	}

	pretty_env_logger::init_custom_env("DISTRINGO_LOG");

	let mut settings = Config::default();

	settings.set_default("server.host", "::")?;
	settings.set_default("server.port", 2020)?;

	settings.merge(config::Environment::with_prefix("DISTRINGO"))?;

	settings.merge(config::File::with_name("config"))?;

	let socket = {
		use core::convert::TryInto;

		let host: IpAddr = settings
			.get_str("server.host")?
			.parse()
			.map_err(|_| distringo::Error::InvalidServerHost)?;
		let port: u16 = settings
			.get_int("server.port")?
			.try_into()
			.map_err(|_| distringo::Error::InvalidServerPort)?;

		SocketAddr::new(host, port)
	};

	server::server(&settings)?.run(socket).await;

	Ok(())
}
