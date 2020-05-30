const Db = require('../../util/db');
const Worker = require('../worker');

class MigrateWorker extends Worker {
    constructor() {
        super();
    }

    async work(options) {
        const db = new Db();
        if (options.options[0] == 'rollback') {
            await db.rollback(options.options[1]);
        } else if (options.options[0] == 'reset') {
            await db.reset();
        } else {            
            await db.takeOnline();
            await db.ensureReadOnlyUser();
            await db.takeOffline(false);
            await db.migrate();
            await db.takeOnline();
            await db.takeOffline();            
        }
        console.log('Migration completed');
    }
}

module.exports = MigrateWorker;