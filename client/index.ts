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

if (true) {
	indexedDB.deleteDatabase("distringo");
}

export type ShapefileSpec = {
	id: string,
	minZoom?: number,
	maxZoom?: number,
}


export type Shapefile = {
	id: string,
	data: string,
}

let shapefiles: Array<ShapefileSpec> = [
	{
		id: "tl_2010_18157_tabblock",
		minZoom: 10.0,
		maxZoom: undefined,
	},
	{
		id: "tl_2010_18157_tabblock",
		minZoom: 10.0,
		maxZoom: undefined,
	},
];

// Pre-seed the database if it is not already set up.
shapefiles.forEach((shapefileSpec) => {
	DB("distringo", 1).then(async (db) => {
		console.debug(`loading shapefile ${shapefileSpec.id}`)

		const id = shapefileSpec.id
		const tx = db.transaction(['shapefiles'], 'readwrite')

		API.shapefile(id)
			.then(data => data.text())
			.then(data => db.put('shapefiles', { id: id, data: data }))
			.then(() => console.debug(`stored shapefile ${id}`))
			.then(() => db.get('shapefiles', id).then(shapefile => {
				console.debug(`drawing shapefile ${shapefile.id}`)
				const data = JSON.parse(shapefile.data)
				L.geoJSON(data, {}).addTo(map)
				console.debug(`finished drawing shapefile ${shapefile.id}`)
			}))
			.then(() => tx.done)
	})
})

console.info("Adding OSM tile set");

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
	attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>'
}).addTo(map);
