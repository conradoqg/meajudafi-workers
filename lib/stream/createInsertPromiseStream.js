const Db = require('../util/db');

const stream = require('stream');

const CONFIG = require('../util/config');

const createInsertPromiseStream = (db, client, progress) => {
    return new stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: async (chunk, e, callback) => {
            if (db) {
                try {
                    const client = await db.pool.connect();
                    try {
                        const newQuery = Db.createUpsertQuery(chunk);

                        const queryPromise = client.query({
                            text: newQuery,
                            rowMode: 'array'
                        });
                        callback(null, queryPromise);
                        await queryPromise;
                    } catch (ex) {
                        console.error('Error: createInsertPromiseStream: transform 1');
                        console.error(ex.stack);
                        callback(ex);
                    } finally {
                        await client.release();
                    }
                } catch (ex) {
                    console.error('Error: createInsertPromiseStream: transform 2');
                    console.error(ex.stack);
                    callback(ex);
                }
            } else {
                try {
                    const newQuery = Db.createUpsertQuery(chunk);

                    const queryPromise = client.query({
                        text: newQuery,
                        rowMode: 'array'
                    });
                    
                    callback(null, queryPromise);
                    await queryPromise;
                } catch (ex) {
                    progress.log(`Error: InsertPromiseStream: ${ex.stack}`);                    
                    callback(ex);
                }
            }
        }
    });
};

module.exports = createInsertPromiseStream;