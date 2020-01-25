const stream = require('stream');

const CONFIG = require('../config');

const createInsertPromiseStream = (db) => {
    return new stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: async (chunk, e, callback) => {
            try {
                const client = await db.pool.connect();
                try {
                    const newQuery = db.createUpsertQuery(chunk);

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
                    client.release();
                }
            } catch (ex) {
                console.error('Error: createInsertPromiseStream: transform 1');
                console.error(ex.stack);
                callback(ex);
            }
        }
    });
};

module.exports = createInsertPromiseStream;