const Db = require('../util/db');

const stream = require('stream');

const CONFIG = require('../util/config');

const createInsertPromiseStream = (client, progress) => {
    return new stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: async (chunk, e, callback) => {
            try {
                const newQuery = Db.createUpsertQuery(chunk);

                const queryPromise = client.query({
                    text: newQuery,
                    rowMode: 'array'
                });

                callback(null, queryPromise);
                await queryPromise;
            } catch (ex) {
                progress && progress.log(`Error: InsertPromiseStream: ${ex.stack}`);
                callback(ex);
            }
        }
    });
};

module.exports = createInsertPromiseStream;
