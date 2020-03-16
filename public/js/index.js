L.Control.Uptown = L.Control.extend({
	onUpdate: function(e) {
		if (this.state.collapsed) {
			this.getContainer().classList.add('collapsed');
		} else {
			this.getContainer().classList.remove('collapsed');
		}
	},

	onClick: function(e) {
		if (this.state.collapsed) {
			this.state.collapsed = false;
		} else {
			this.state.collapsed = true;
		}

		this.onUpdate(e);
	},

	onAdd: function(map) {
		let element = L.DomUtil.create('div');

		element.classList.add('uptown-controls');
		element.classList.add('collapsed');

		L.DomEvent.on(element, { click: this.onClick }, this);

		this.state = {
			collapsed: true,
		};

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
