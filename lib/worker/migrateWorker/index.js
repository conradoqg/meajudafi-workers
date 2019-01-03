const Db = require('../../util/db');
const Worker = require('../worker');

class MigrateWorker extends Worker {
    constructor() {
        super();
    }

    async work(options) {
        const db = new Db();
        if (options[0] == 'rollback') {
            await db.rollback(options[1]);
        } else if (options[0] == 'reset') {
            await db.reset();
        } else {
            await db.migrate();
        }
        console.log('Migration completed');
    }
}

module.exports = MigrateWorker;