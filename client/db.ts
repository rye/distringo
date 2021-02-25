import { openDB, deleteDB, wrap, unwrap } from 'idb';

export const DB = async (name: string, version: number) => {
	await openDB(name, version, {
		upgrade(db: unknown, oldVersion: number, newVersion: number, transaction: unknown) {
			switch ([oldVersion, newVersion]) {
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
	})
};
