const stream = require('stream');
const CONFIG = require('../util/config');
const processFund = require('../thread/processFund');

const { DynamicPool } = require('node-worker-threads-pool');

const createFundCalculatorStream = (connectionPool) => {
    const threadPool = new DynamicPool(CONFIG.MAX_PARALLEL_THREADS);
    const threadsPromise = [];
    return stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: async (chunk, e, callback) => {
            try {
                if (CONFIG.PARALLEL_PROCESS) {
                    const threadPromise = threadPool.exec({
                        task: async () => {
                            require('./lib/util/chainableError').replaceOriginalWithChained();
                            const processFund = require('./lib/thread/processFund');
                            return await processFund(this.workerData.chunk);                            
                        },
                        workerData: { chunk },
                    });
                    threadsPromise.push(threadPromise);
                    callback(null, threadPromise);
                    await threadPromise;
                } else {
                    await processFund(chunk, callback, connectionPool);
                }
            } catch (ex) {
                try {
                    threadPool.destroy();
                } catch (ex) {
                    // Do nothing ¯ \ _ (ツ) _ / ¯
                }
                callback(ex);
            }
        },
        flush: async (cb) => {
            try {
                await Promise.allSettled(threadsPromise);
                cb();
            } catch (ex) {
                cb(ex);
            } finally {
                threadPool.destroy();
            }
        }
    });
};

module.exports = createFundCalculatorStream;