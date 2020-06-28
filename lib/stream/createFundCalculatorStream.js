const stream = require('stream');
const CONFIG = require('../util/config');
const processFund = require('../thread/processFund');

const { DynamicPool } = require('node-worker-threads-pool');

const createFundCalculatorStream = (connectionPool, progress) => {    
    const threadPool = new DynamicPool(CONFIG.MAX_PARALLEL_THREADS);
    const threadsPromise = [];    
    return stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: async (chunk, e, callback) => {
            try {
                if (CONFIG.PARALLEL_PROCESS) {                    
                    const res = threadPool.exec({
                        task: () => {
                            const processFund = require('./lib/thread/processFund');
                            return processFund(this.workerData.chunk);
                        },
                        workerData: { chunk },
                    });
                    threadsPromise.push(res);                    
                    callback(null, res);
                } else {
                    processFund(chunk, callback, connectionPool);
                }
            } catch (ex) {
                progress.log(`Error: createFundCalculatorStream: transform 1: ${ex.stack}`);
                callback(ex);
            }
        },
        flush: async (cb) => {
            if (CONFIG.PARALLEL_PROCESS) {
                try {
                    await Promise.allSettled(threadsPromise);
                } finally {
                    threadPool.destroy();
                }
            }
            cb();
        }
    });
};

module.exports = createFundCalculatorStream;