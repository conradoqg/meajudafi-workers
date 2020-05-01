const stream = require('stream');
const CONFIG = require('../config');
const processFund = require('../thread/processFund');

const { DynamicPool } = require("node-worker-threads-pool");

const pool = new DynamicPool(CONFIG.MAX_PARALLEL_THREADS);

const createFundCalculatorStream = (db) => {
    return stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: async (chunk, e, callback) => {
            try {
                if (CONFIG.PARALLEL_PROCESS) {
                    const res = pool.exec({
                        task: () => {
                            const processFund = require('./lib/thread/processFund');
                            return processFund(this.workerData.CONFIG, this.workerData.chunk);
                        },
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