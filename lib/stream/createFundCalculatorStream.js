const stream = require('stream');
const CONFIG = require('../config');
const processFund = require('./processFund');

const { DynamicPool } = require("node-worker-threads-pool");

const pool = new DynamicPool(16);

const createFundCalculatorStream = (db) => {
    return stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: async (chunk, e, callback) => {
            try {
                if (true) {
                    const func = () => {
                        const processFund = require('./lib/stream/processFund');
                        return processFund(this.workerData.CONFIG, this.workerData.chunk);
                    };

                    const res = pool.exec({
                        task: func,
                        workerData: { CONFIG, chunk },
                    });

                    callback(null, res);
                } else {
                    processFund(CONFIG, chunk, callback, db);
                }
            } catch (ex) {
                console.error('Error: createFundCalculatorStream: transform 1');
                console.error(ex);
                callback(ex);
            }
        }
    });
};

module.exports = createFundCalculatorStream;