import { API, API_ROUTE } from './api'

let container = document.getElementById('map');

import { DB } from './db'

import * as L from 'leaflet'


console.info("Initializing the map");

let map = L.map(container, {
	center: [0.0, 0.0],
	zoom: 0,
	zoomSnap: 0.25,
	zoomDelta: 0.25,
	boxZoom: false,
	doubleClickZoom: false,
});

DB("distringo", 1).then(db => console.debug(db));

type Shapefile = {
	id: string,
	data: { id: string },
	minZoom?: number,
	maxZoom?: number,
}

let shapefiles: Array<Shapefile> = [
	{
		id: "tl_2010_18157_tabblock",
		data: { id: "tl_2010_18157_tabblock" },
		minZoom: 10.0,
		maxZoom: undefined,
	},
];

for (let shapefile of shapefiles) {
	if (shapefile.data && typeof shapefile.data.id === "string") {
		console.debug(`Processing shapefile ${shapefile.data.id}`)

		let id = shapefile.data.id

		API.shapefile(id)
			.then(data => data.json())
			.then(object => {
				L.geoJSON(object, {}).addTo(map)
			})
	}
}

console.info("Adding OSM tile set");

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
	attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>'
}).addTo(map);
