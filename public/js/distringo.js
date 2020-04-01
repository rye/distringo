const VERSION = 'v0.1.0';

L.Control.Distringo = L.Control.extend({
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

		element.classList.add('distringo-controls');
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

L.control.distringo = function(opts) { return new L.Control.Distringo(opts); }

function getMapState() {
	try {
		return JSON.parse(window.sessionStorage.mapState);
	} catch {
		return {};
	}
};

function getClientState() {
	try {
		return JSON.parse(window.sessionStorage.clientState);
	} catch {
		return {};
	}
}

const API = (version, endpoint) => {
	switch(version) {
		case 'v0.1.0':
			return `/api/v0/${endpoint}`;
			break;
		default:
			throw `invalid API version ${version}`
	}
}
