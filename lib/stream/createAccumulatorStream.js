const stream = require('stream');

const CONFIG = require('../config');

const createAccumulatorStream = () => {
    let valuesPerTableDefinition = {};
    return stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: function (chunk, e, callback) {
            if (!valuesPerTableDefinition[chunk.table]) {
                valuesPerTableDefinition[chunk.table] = {
                    table: chunk.table,
                    primaryKey: chunk.primaryKey,
                    values: []
                };
            }
            valuesPerTableDefinition[chunk.table].values.push(chunk.fields);
            callback();
        },
        flush: function (callback) {
            Object.values(valuesPerTableDefinition).map(tableDefinition => this.push(tableDefinition));
            callback();
        }
    });
};

module.exports = createAccumulatorStream;