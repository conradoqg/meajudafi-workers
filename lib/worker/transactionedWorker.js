const Db = require('../util/db');
const DefaultProgress = require('../progress/defaultProgress');
const Worker = require('./worker');
const WorkerError = require('./workerError');
const getAllMethods = require('./getAllMethods');
const Sentry = require('@sentry/node');

class TransactionedWorker extends Worker {
    async work(options) {
        const methods = getAllMethods(this);

        const mainProgress = new DefaultProgress(this.id);
        let isClientErrored = false;

        const transaction = Sentry.startTransaction({
            op: 'work',
            name: this.id
        });

        Sentry.configureScope((scope) => {
            scope.setSpan(transaction);
        });

        try {

            mainProgress.start(methods.length + 1);

            const db = new Db();

            await db.takeOnline();

            try {
                const mainClient = await db.pool.connect();
                mainClient.on('error', err => {
                    mainProgress.log(`Database client errored: ${err.stack}`);
                    isClientErrored = true;
                });

                try {

                    await mainClient.query('BEGIN TRANSACTION');

                    let result = options;

                    for await (const method of methods) {
                        const taskProgress = new DefaultProgress(`${this.id}.${method}`);

                        let span = transaction.startChild({ op: method });

                        try {
                            result = await this[method](mainClient, taskProgress, result);

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

                    await mainClient.query('COMMIT');
                } catch (ex) {
                    if (!isClientErrored) await mainClient.query('ROLLBACK');
                    throw ex;
                } finally {
                    await mainClient.release();
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

module.exports = TransactionedWorker;