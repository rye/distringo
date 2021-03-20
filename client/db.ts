import { openDB, deleteDB, wrap, unwrap, DBSchema, IDBPDatabase, IDBPTransaction } from 'idb'

import type { Shapefile } from './index'

interface DistringoDB extends DBSchema {
	shapefiles: {
		key: string,
		value: Shapefile,
	}
}

export const DB = async (name: string, version: number) => await openDB<DistringoDB>(name, version, {
	upgrade(db: IDBPDatabase<DistringoDB>, oldVersion: number, newVersion: number, transaction: unknown) {
		console.debug(`Upgrading db ${name} from v${oldVersion} to v${newVersion}`)

		if (oldVersion === 0) {
			console.debug(`Initializing database ${name}...`)

			db.createObjectStore('shapefiles', {
				keyPath: 'id',
			});
		} else {
			switch ([oldVersion, newVersion]) {
				default:
					console.error(`don't know how to upgrade from version ${oldVersion} to ${newVersion}`);
			}
		}
	},

	blocked() {

	},

	blocking() {
	},

	terminated() {
	},
});
