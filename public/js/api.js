const API_ROUTE = (part) => `${apiUri}/${part}`

const API = {
	sessions: () => fetch(API_ROUTE("sessions")),
	shapefiles: () => fetch(API_ROUTE("shapefiles")),
	shapefile: (id) => fetch(API_ROUTE(`shapefiles/${id}`)),
};
