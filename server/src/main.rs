pub mod server;

#[tokio::main]
async fn main() -> distringo::Result<()> {
	if std::env::var("DISTRINGO_LOG").ok().is_none() {
		std::env::set_var("DISTRINGO_LOG", "info");
	}

	pretty_env_logger::init_custom_env("DISTRINGO_LOG");

	let settings = {
		use config::{Config, Environment, File};

		let mut settings = Config::default();

		settings.set_default("server.host", "::")?;
		settings.set_default("server.port", 2020)?;

		settings.merge(Environment::with_prefix("DISTRINGO"))?;

		settings.merge(File::with_name("config"))?;

		settings
	};

	let mut plan: server::ExecutionPlan = settings.into();
	plan.prepare()?;
	plan.execute().await
}
