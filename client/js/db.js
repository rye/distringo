import { openDB, deleteDB, wrap, unwrap } from 'idb';

export const DB = async (name, version) => await openDB(name, version, {
	upgrade(db, oldVersion, newVersion, transaction) {
		switch([oldVersion, newVersion]) {
			default:
				console.error(`don't know how to upgrade from version ${oldVersion} to ${newVersion}`);
		}
	},

	blocked() {

	},

	blocking() {
	},

	terminated() {
	},
});
