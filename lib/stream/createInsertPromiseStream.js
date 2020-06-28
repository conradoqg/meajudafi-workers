const Db = require('../util/db');

const stream = require('stream');

const CONFIG = require('../util/config');

const createInsertPromiseStream = (client) => {
    return new stream.Writable({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        write: async (chunk, e, callback) => {
            try {
                const newQuery = Db.createUpsertQuery(chunk);

                try {
                    const queryPromise = client.query({
                        text: newQuery,
                        rowMode: 'array'
                    });                 
                    await queryPromise;
                    callback();
                } catch (ex) {                    
                    callback(new Error(`Query "${CONFIG.QUERY_LOG_SIZE ? newQuery.substr(0, CONFIG.QUERY_LOG_SIZE) : newQuery}..." failed`, ex));
                }
            } catch (ex) {
                callback(ex);
            }
        }
    });
};

module.exports = createInsertPromiseStream;
