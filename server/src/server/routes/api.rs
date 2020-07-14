use shapefiles::{Shapefile, ShapefileConfiguration};

use core::convert::TryInto;
use std::collections::HashMap;
use std::sync::Arc;

use warp::Filter;

pub mod shapefiles;

pub fn shapefiles(
	cfg: &config::Config,
) -> distringo::Result<warp::filters::BoxedFilter<(impl warp::Reply,)>> {
	let shapefiles: HashMap<String, config::Value> = cfg.get_table("shapefiles")?;

	let shapefiles: HashMap<String, Shapefile> = shapefiles
		.iter()
		.filter_map(
			|(id, value): (&String, &config::Value)| -> Option<(String, Shapefile)> {
				// TODO(rye): avoid clone by iterating over keys and using remove?
				let value: config::Value = value.clone();
				// TODO(rye): handle error a bit better
				let config: ShapefileConfiguration = value.try_into().expect("invalid configuration");
				let shapefile: distringo::Result<Shapefile> = config.try_into();
				if let Ok(shapefile) = shapefile {
					Some((id.to_string(), shapefile))
				} else {
					log::warn!("Error parsing shapefile {}: {:?}", id, shapefile);
					None
				}
			},
		)
		.collect();

	let shapefiles: Arc<HashMap<String, Shapefile>> = Arc::new(shapefiles);

	// GET /api/v0/shapefiles
	let shapefiles_index = {
		let shapefiles = shapefiles.clone();
		warp::get()
			.and(warp::path::end())
			.map(move || shapefiles::index(&shapefiles))
	};
	// GET /api/v0/shapefiles/:id
	let shapefiles_show = {
		#[allow(clippy::redundant_clone)]
		let shapefiles = shapefiles.clone();
		warp::get()
			.and(warp::path!(String))
			.map(move |id: String| shapefiles::show(&shapefiles, &id))
			.with(warp::compression::gzip())
	};
	// ... /api/v0/shapefiles/...
	let shapefiles = warp::any()
		.and(warp::path!("shapefiles" / ..))
		.and(shapefiles_index.or(shapefiles_show))
		.boxed();

	Ok(shapefiles)
}

pub mod v0 {
	pub use super::shapefiles;
}

pub fn api(
	cfg: &config::Config,
) -> distringo::Result<warp::filters::BoxedFilter<(impl warp::Reply,)>> {
	let shapefiles = shapefiles(cfg)?;

	let api = warp::path("api");
	let api_v0 = api.and(warp::path("v0"));

	let gets = shapefiles;

	Ok(warp::any().and(api.or(api_v0).unify()).and(gets).boxed())
}
