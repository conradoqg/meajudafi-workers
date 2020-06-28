const stream = require('stream');

const CONFIG = require('../util/config');

const createWaitForPromisesStream = () => {
    return new stream.Writable({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        write: async (chunk, e, callback) => {
            try {                
                await chunk;
                callback();
            } catch (ex) {                
                callback(ex);
            }
        }
    });
};

module.exports = createWaitForPromisesStream;