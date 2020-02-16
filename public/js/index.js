L.Control.Uptown = L.Control.extend({
	onAdd: function(map) {
		let element = L.DomUtil.create('div');
		return element;
	},

	onRemove: function(map) {
		// Nothing to do here...
	}
});

L.control.uptown = function(opts) { return new L.Control.Uptown(opts); }
