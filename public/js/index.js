L.Control.Uptown = L.Control.extend({
	onAdd: function(map) {
		let element = L.DomUtil.create('div');
		element.classList.add('uptown-controls');
		return element;
	},

	onRemove: function(map) {
		// Nothing to do here...
	}
});

L.control.uptown = function(opts) { return new L.Control.Uptown(opts); }

function getMapState() {
	try {
		return JSON.parse(window.sessionStorage.mapState);
	} catch {
		return {};
	}
};
