const Db = require('../util/db');
const DefaultProgress = require('../progress/defaultProgress');
const Worker = require('./worker');
const WorkerError = require('./workerError');
const getAllMethods = require('./getAllMethods');
const Sentry = require('@sentry/node');

class PooledWorker extends Worker {
    async work(options) {
        const methods = getAllMethods(this);

        const mainProgress = new DefaultProgress(this.id);

        const transaction = Sentry.startTransaction({
            op: 'work',
            name: this.id
        });

        try {

            mainProgress.start(methods.length + 1);

            const db = new Db();

            await db.takeOnline();

            try {

                let result = options;

                for await (const method of methods) {
                    const taskProgress = new DefaultProgress(`${this.id}.${method}`);

                    let span = transaction.startChild({ op: method });

                    try {
                        result = await this[method](db.pool, taskProgress, result);

                        taskProgress.end();
                    } catch (ex) {
                        taskProgress.error();
                        Sentry.captureException(ex);
                        throw new WorkerError(`Step ${this.id}.${method} failed`, ex);
                    } finally {
                        span.finish();
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
            Sentry.captureException(ex);
            mainProgress.error();
        } finally {
            transaction.finish();
        }
    }
}

module.exports = PooledWorker;