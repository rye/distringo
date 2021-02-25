const apiUri = `${window.location.origin}/api/v0`;

export const API_ROUTE = (part: string) => `${apiUri}/${part}`

export const API = {
	sessions: () => fetch(API_ROUTE("sessions")),
	shapefiles: () => fetch(API_ROUTE("shapefiles")),
	shapefile: (id: string) => fetch(API_ROUTE(`shapefiles/${id}`)),
};
