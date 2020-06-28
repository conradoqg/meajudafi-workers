const uuidv1 = require('uuid/v1');
const stream = require('stream');

const CONFIG = require('../util/config');

const createBatchTransformStream = (table, primaryKey, batchSize = CONFIG.batchSize) => {
    let values = {};

    return new stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: (chunk, e, callback) => {
            try {
                chunk.id = uuidv1();
                const hash = Array.isArray(primaryKey) ? primaryKey.map(key => chunk[key]).join('') : chunk.id;
                values[hash] = chunk;                

                if (Object.keys(values).length >= batchSize) {
                    const valuesCopy = values;
                    values = {};
                    callback(null, {
                        table,
                        primaryKey,
                        values: Object.values(valuesCopy)
                    });
                } else {
                    callback(null, null);
                }
            } catch (ex) {
                callback(ex);                
            }
        },
        flush: (callback) => {
            try {
                if (Object.keys(values).length > 0) {
                    const valuesCopy = values;
                    values = {};
                    callback(null, {
                        table,
                        primaryKey,
                        values: Object.values(valuesCopy)
                    });
                } else callback();
            } catch (ex) {
                callback(ex);                
            }
        }
    });
};

module.exports = createBatchTransformStream;