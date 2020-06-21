const Db = require('../util/db');
const DefaultProgress = require('../progress/defaultProgress');

class ManagedWorker {
    id = null
    tasks = []

    constructor(id) {
        this.id = id;
    }

    addTask(taskFn) {
        this.tasks.push(taskFn);
    }

    async work() {
        const mainProgress = new DefaultProgress(this.id);
        let isClientErrored = false;

        try {

            mainProgress.start(this.tasks.length + 1);

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

                    let result = null;

                    for await (const task of this.tasks) {
                        const taskProgress = new DefaultProgress(`${this.id}.${task.id}`);

                        try {
                            result = await task.fn(mainClient, taskProgress, result);

                            taskProgress.end();
                        } catch (ex) {
                            taskProgress.error();
                            throw ex;
                        }

                        mainProgress.step();
                    }

                    await mainClient.query('COMMIT');
                } catch (ex) {
                    if (!isClientErrored) {
                        await mainClient.query('ROLLBACK');
                    }
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

module.exports = ManagedWorker;