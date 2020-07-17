use shapefiles::{Shapefile, ShapefileConfiguration};

use std::collections::HashMap;

use warp::Filter;

pub mod shapefiles;

mod cache {
	use super::{Shapefile, ShapefileConfiguration};
	use std::collections::HashMap;
	use std::convert::TryInto;
	use std::mem::MaybeUninit;
	use std::sync::Once;

	mod internal {
		use super::{HashMap, MaybeUninit, Once, Shapefile};
		pub(super) static mut CACHE: MaybeUninit<HashMap<String, Shapefile>> = MaybeUninit::uninit();
		pub(super) static CACHE_INIT: Once = Once::new();
	}

	pub(super) fn get_cache(cfg: &config::Config) -> &'static HashMap<String, Shapefile> {
		// SAFETY: Here we use the `static mut`-`Once` pattern to ensure that the code
		// to initialize the contents of LOADED_SHAPEFILES beyond the starting value
		// `HashMap::new()` is called only once.
		//
		// We use `MaybeUninit` to get a blank spot to put our `HashMap` into.
		//
		//
		internal::CACHE_INIT.call_once(|| {
			unsafe {
				internal::CACHE = MaybeUninit::new(HashMap::new());
			}

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
					.for_each(|(id, shp): (String, Shapefile)| unsafe {
						(*internal::CACHE.as_mut_ptr()).insert(id, shp);
					});
			};
		});
		unsafe { &*internal::CACHE.as_ptr() }
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

	Ok(warp::any().and(api.or(api_v0)).unify().and(gets).boxed())
}
