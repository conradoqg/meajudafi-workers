const Db = require('../util/db');
const DefaultProgress = require('../progress/defaultProgress');
const Worker = require('./worker');
const WorkerError = require('./workerError');
const getAllMethods = require('./getAllMethods');

class PooledWorker extends Worker {
    async work(options) {
        const methods = getAllMethods(this);

        const mainProgress = new DefaultProgress(this.id);        

        try {

            mainProgress.start(methods.length + 1);

            const db = new Db();

            await db.takeOnline();

            try {

                let result = options;

                for await (const method of methods) {
                    const taskProgress = new DefaultProgress(`${this.id}.${method}`);

                    try {
                        result = await this[method](db.pool, taskProgress, result);

                        taskProgress.end();
                    } catch (ex) {
                        taskProgress.error();
                        throw new WorkerError(`Step ${this.id}.${method} failed`, ex);
                    }

                    mainProgress.step();
                }
            } finally {
                await db.takeOffline(true, mainProgress);
            }
            mainProgress.step();

            mainProgress.end();
        } catch (ex) {
            mainProgress.log((new WorkerError(`Worker ${this.id} failed`, ex)).stack);
            mainProgress.error();
        }
    }
}

module.exports = PooledWorker;