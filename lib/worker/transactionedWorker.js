const Db = require('../util/db');
const DefaultProgress = require('../progress/defaultProgress');
const Worker = require('./worker');
const getAllMethods = require('./getAllMethods');

class TransactionedWorker extends Worker {
    async work(options) {
        const methods = getAllMethods(this);

        const mainProgress = new DefaultProgress(this.id);
        let isClientErrored = false;

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

                        try {
                            result = await this[method](mainClient, taskProgress, result);

                            taskProgress.end();
                        } catch (ex) {
                            taskProgress.error();
                            throw ex;
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
            mainProgress.log(ex.stack);
            mainProgress.error();            
        }
    }
}

module.exports = TransactionedWorker;