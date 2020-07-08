const pg = require('pg');
const CONFIG = require('../util/config');
const Db = require('../util/db');

let pool = new pg.Pool({
    connectionString: CONFIG.CONNECTION_STRING,
    max: 2
});
pool.on('error', (err) => {
    // Do nothing
    console.log('Error on DatabaseOutput', err);
});

process.on('beforeExit', async () => {    
    await Promise.allSettled(outputPromises);    
    console.log('Promises settled on progress');
});

const outputPromises = [];

class DatabaseOutput {    
    write(progressTracker, prettyProgressTracker) {
        const progressUpsertQuery = Db.createUpsertQuery({
            table: 'progress',
            primaryKey: ['path'],
            values: [{
                'path': progressTracker.id,
                'data': JSON.stringify({
                    progressTracker,
                    prettyProgressTracker
                })
            }]
        });

        outputPromises.push(pool.query(progressUpsertQuery).catch(ex => ex));
    }
}

module.exports = DatabaseOutput;
