window.clientState = getClientState();

const setSession = (formData) => {
	console.debug(formData);

	let session = formData.get("session");

	console.log(`setting session to ${session}`);

	window.clientState = window.clientState || {};
	window.clientState.session = session;

	window.sessionStorage.clientState = JSON.stringify(window.clientState);

	location.reload()
}

if(clientState && clientState.session) {
	let session = clientState.session;

	console.debug(`Client is current in session ${session}`)

	let mapState = getMapState();

	let [center, zoom] = [
		mapState.center || {lat: 39.8333333, lng: -98.585522},
		mapState.zoom || 3.0,
	];

	let container = document.getElementById('container');
	container.classList.add('in-session');

	let map = L.map(container, {
		center: center,
		zoom: zoom,
		zoomSnap: 0.25,
		zoomDelta: 0.25,
		boxZoom: false,
		doubleClickZoom: false,
	});

	L.control.distringo({ position: 'topright' }).addTo(map);

	window.addEventListener('unload', e => {
		let mapState = {
			center: map.getCenter(),
			zoom: map.getZoom(),
		};

		window.sessionStorage.mapState = JSON.stringify(mapState);
	});

	fetch('tl_2010_18157_tabblock10.geojson')
		.then(response => response.json())
		.then(geojsonObject => {
			L.geoJSON(geojsonObject, {
				style: (feature) => { return { weight: 1, color: '#404040', }; },
				onEachFeature: (feature, layer) => {
					layer.bindPopup((layer) => {
						let table = L.DomUtil.create('table');

						let properties = layer.feature.properties;

						Object.keys(properties).forEach(key => {
							let row = L.DomUtil.create('tr', '', table);
							let label = L.DomUtil.create('td', '', row);
							label.innerHTML = key;
							let value = L.DomUtil.create('td', '', row);
							value.innerHTML = properties[key];
						});

						return table;
					}, {
						closeOnClick: false,
						autoClose: true,
					});

					layer.on('popupopen', (e) => {
						layer.originalOptions = Object.assign({}, layer.options);
						layer.setStyle({ fill: false, weight: 4 });
					});

					layer.on('popupclose', (e) => {
						layer.setStyle(layer.originalOptions);
						layer.redraw();
						layer.originalOptions = undefined;
					});

					layer.on('click', (e) => {
						if(e.originalEvent.shiftKey) {
							console.log("Clicked with shift held!");
							if(layer.isPopupOpen()) {
								layer.closePopup();
							}
						}
					});

					layer.on('mousedown', (e) => {
						map.isBeingDragged = true;
					});

					layer.on('mousemove', (e) => {
						if(map.isBeingDragged && map.selecting) {
							console.debug(e);
							e.target.originalOptions = Object.assign({}, e.target.options);
							e.target.setStyle({ fill: true, fillOpacity: 0.5,  fillColor: '#014A01', weight: 2, color: '#308C30', dashArray: '4 8' });
							e.target.selected = true;
						}
					});

					layer.on('mouseup', (e) => {
						map.isBeingDragged = false;
					});
				}
			}).addTo(map);
		});

	document.addEventListener('keydown', (e) => {
		if(e.key == "Shift") {
			container.classList.add("selecting");
			map.selecting = true;
		}
	});

	document.addEventListener('keyup', (e) => {
		if(e.key == "Shift") {
			container.classList.remove("selecting");
			map.selecting = false;
		}
	});

	L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
		attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>'
	}).addTo(map);
} else {
	console.log("Client is not in a session currently.");

	let container = document.getElementById('container');
	container.classList.add('session-selection');

	let div = document.createElement('div');
	div.classList.add('session-menu');

	{
		let title = document.createElement('h1');
		title.innerHTML = 'distringo';
		div.appendChild(title);
	}

	{
		let form = document.createElement('form');
		form.setAttribute('action', 'javascript:;');

		let label = document.createElement('label');
		label.for = 'session-select';
		label.innerHTML = 'Select a session:';

		let select = document.createElement('select');
		select.id = 'session-select';
		select.name = 'session';
		select.disabled = true;

		const getSessions = () => {
			const populateSessions = (sessions) => {
				for (session of sessions) {
					console.debug("Processing session: ", session);

					if(!session.id)
						throw "missing session id"

					if(!session.name)
						throw "missing session name"

					let option = document.createElement('option');
					option.value = session.id;
					option.innerHTML = session.name;

					select.appendChild(option);
				}
			};

			fetch(API(VERSION, 'sessions'))
				.then(response => response.json())
				.then(json => populateSessions(json))
				.then(() => { select.disabled = false; })
		};

		getSessions();

		form.appendChild(label);
		form.appendChild(select);

		let submit = document.createElement('input');
		submit.type = 'submit';
		submit.value = 'Go';

		form.appendChild(submit);

		form.addEventListener('submit', (e) => {
			console.log("user submitted form");

			e.preventDefault();

			new FormData(form);
		});

		form.addEventListener('formdata', (e) => {
			console.log("formdata fired");

			console.debug(e);

			setSession(e.formData);
		});

		div.appendChild(form);
	}

	container.appendChild(div);
}
