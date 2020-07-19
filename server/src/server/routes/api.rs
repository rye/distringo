use shapefiles::{Shapefile, ShapefileConfiguration};

use std::collections::HashMap;

use once_cell::sync::OnceCell;
use warp::Filter;

pub mod shapefiles;

mod cache {
	use super::{OnceCell, Shapefile, ShapefileConfiguration};
	use std::collections::HashMap;
	use std::convert::TryInto;

	static CACHE: OnceCell<HashMap<String, Shapefile>> = OnceCell::new();

	pub(super) fn get_cache(cfg: &config::Config) -> &'static HashMap<String, Shapefile> {
		let mut cache = HashMap::new();

		if let Ok(configuration) = cfg.get_table("shapefiles") {
			configuration
				.iter()
				.filter_map(|(id, value)| -> Option<(String, Shapefile)> {
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
				})
				.for_each(|(id, shp): (String, Shapefile)| {
					cache.insert(id, shp);
				});
		};

		CACHE.set(cache).expect("cache already initialized");

		CACHE.get().unwrap()
	}
}

pub fn shapefiles(
	cfg: &config::Config,
) -> distringo::Result<warp::filters::BoxedFilter<(impl warp::Reply,)>> {
	let loaded_shapefiles: &'static HashMap<String, Shapefile> = cache::get_cache(cfg);

	// GET /api/v0/shapefiles
	let shapefiles_index = warp::get()
		.and(warp::path::end())
		.map(move || shapefiles::index(loaded_shapefiles));

	// GET /api/v0/shapefiles/:id
	let shapefiles_show = warp::get()
		.and(warp::path!(String))
		.map(move |id: String| shapefiles::show(loaded_shapefiles, &id))
		.with(warp::compression::gzip());

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

	Ok(warp::any().and(api_v0.or(api).unify()).and(gets).boxed())
}
