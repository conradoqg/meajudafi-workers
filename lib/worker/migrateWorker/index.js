const Db = require('../../util/db');

const fs = require('fs-extra');

const Worker = require('../worker');

class MigrateWorker extends Worker {
    constructor() {
        super();
    }

    async moveProcessedListToDB(db) {
        const client = await db.pool.connect();
        try {
            const PROCESSEDLIST_FILENAME = './db/processedList.json';

            if (fs.existsSync(PROCESSEDLIST_FILENAME)) {
                const processedList = fs.readJSONSync(PROCESSEDLIST_FILENAME);

                const processedURLUpsertQuery = Db.createUpsertQuery({
                    table: 'processed_url',
                    primaryKey: ['url'],
                    values: Object.keys(processedList).map(key => {
                        return {
                            'url': key,
                            'etag': processedList[key]
                        };
                    })
                });
                await client.query(processedURLUpsertQuery);

                fs.removeSync('./db');
            }
        } catch (ex) {
            // Do nothing, probably the table doesn't exist yet.
        } finally {
            await client.release();
        }
    }

    async work(options) {
        const db = new Db();
        if (options.options[0] == 'rollback') {
            await db.rollback(options.options[1]);
        } else if (options.options[0] == 'reset') {
            await db.reset();
        } else {
            await db.takeOnline();
            await db.ensureReadOnlyUser();
            await db.takeOffline(false);
            await db.migrate();
            await db.takeOnline();
            await this.moveProcessedListToDB(db);
            await db.takeOffline();
        }
        console.log('Migration completed');
    }
}

module.exports = MigrateWorker;