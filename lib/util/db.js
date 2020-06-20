const pg = require('pg');
const types = require('pg').types;
const dayjs = require('dayjs');
const parse = require('pg-connection-string').parse;
const PgMigrate = require('@urbica/pg-migrate');
const path = require('path');

const packageJSON = require('../../package.json');
const CONFIG = require('./config');

const DATE = 1082;

types.setTypeParser(DATE, val => val === null ? null : dayjs(new Date(parseInt(val.substring(0, 4)), parseInt(val.substring(5, 7)) - 1, parseInt(val.substring(8, 10)))));

const CACHE = {};

const createReadOnlyUser = `
DO
$do$
BEGIN
    IF NOT EXISTS (
        SELECT
        FROM   pg_catalog.pg_roles
        WHERE  rolname = '${CONFIG.READONLY_USERNAME}') THEN

        CREATE ROLE ${CONFIG.READONLY_USERNAME} WITH LOGIN ENCRYPTED PASSWORD '${CONFIG.READONLY_PASSWORD}' NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';        
   END IF;
END
$do$;
`;

class Db {
    async takeOnline() {
        this.pool = new pg.Pool({
            connectionString: CONFIG.CONNECTION_STRING,
            max: CONFIG.POOL_SIZE
        });
        this.pool.on('error', (err) => {
            console.error('Error: Db: takeOnline');
            console.error('An idle client has experienced an error\n', err.stack);
        });
        return this.updateWorkerInfo();
    }

    async takeOffline(refresh = true) {
        refresh && await this.refreshMaterializedViews();
        return this.pool.end();
    }

    async updateWorkerInfo() {

        try {
            const versionUpsertQuery = this.createUpsertQuery({
                table: 'about',
                primaryKey: ['what'],
                values: [{
                    'what': 'worker',
                    'info': JSON.stringify({
                        'version': packageJSON.version
                    })
                }]
            });
            await this.pool.query(versionUpsertQuery);
        } catch (ex) {
            // Do nothing, probably the table doesn't exist yet.
        }
    }

    async ensureReadOnlyUser() {
        const client = await this.pool.connect();
        try {
            await client.query(createReadOnlyUser);
        } finally {
            await client.release();
        }
    }

    async refreshMaterializedViews() {
        const views = [
            'icf_with_xf_and_bf_and_iry_and_f_of_last_year',
            'icf_with_xf_and_bf_and_mf_and_iry_and_f_of_last_year',
            'irm_timeseries',
            'running_days',
            'running_days_with_indicators',
            'funds_enhanced'
        ];
        const DefaultProgress = require('../progress/defaultProgress');

        const progress = new DefaultProgress('Database');

        try {

            const client = await this.pool.connect();
            client.on('error', err => {
                progress.log(`Database client errored: ${err.stack}`);
            });

            try {

                progress.start(views.length);

                for await (const view of views) {

                    await client.query(`REFRESH MATERIALIZED VIEW ${view} WITH DATA;`);

                    progress.step();
                }

                progress.end();
            } finally {
                await client.release();
            }
        } catch (ex) {
            progress.error();
            progress.log(ex.stack);
        }
    }

    async migrate() {
        var config = parse(CONFIG.CONNECTION_STRING);

        const pgMigrate = new PgMigrate({
            database: config.database,
            host: config.host,
            port: config.port,
            user: config.user,
            password: config.password,
            migrationsDir: path.join(__dirname, '../../migrations'),
            verbose: true
        });

        await pgMigrate.connect();
        await pgMigrate.migrate();
        await pgMigrate.end();
    }

    async rollback(amount = null) {
        var config = parse(CONFIG.CONNECTION_STRING);

        const pgMigrate = new PgMigrate({
            database: config.database,
            host: config.host,
            port: config.port,
            user: config.user,
            password: config.password,
            migrationsDir: path.join(__dirname, '../../migrations'),
            verbose: true
        });

        await pgMigrate.connect();
        await pgMigrate.rollback(amount);
        await pgMigrate.end();
    }

    async reset() {
        const config = parse(CONFIG.CONNECTION_STRING);

        const pgMigrate = new PgMigrate({
            database: config.database,
            host: config.host,
            port: config.port,
            user: config.user,
            password: config.password,
            migrationsDir: path.join(__dirname, '../../migrations'),
            verbose: true
        });

        await pgMigrate.connect();
        await pgMigrate.reset();
        await pgMigrate.end();
    }

    static createUpsertQuery(data) {
        let valuesPart = '';
        let fieldsPart = null;
        let updateFieldsPart = null;
        const onConflictPart = (Array.isArray(data.primaryKey) ? data.primaryKey.join(', ') : data.primaryKey);
        for (let index = 0; index < data.values.length; index++) {
            const row = data.values[index];

            if (index != 0) valuesPart += ',';
            else {
                if (!CACHE[data.table]) {
                    CACHE[data.table] = {
                        fieldsPart: Object.keys(data.values[0]).join(', '),
                        updateFieldsPart: Object.keys(data.values[0]).slice(0).map(value => `${value} = excluded.${value}`).join(', ')
                    };
                }
                fieldsPart = CACHE[data.table].fieldsPart;
                updateFieldsPart = CACHE[data.table].updateFieldsPart;
            }

            let values = '';
            for (const fieldKey in row) {
                if (values != '') values += ', ';
                const field = row[fieldKey];
                values += (field != null ? `'${field}'` : 'null');
            }
            valuesPart += `(${values})`;
        }

        let newQuery = null;
        if (data.primaryKey)
            newQuery = `INSERT INTO ${data.table} (${fieldsPart}) VALUES ${valuesPart} ON CONFLICT (${onConflictPart}) DO UPDATE SET ${updateFieldsPart}`;
        else
            newQuery = `INSERT INTO ${data.table} (${fieldsPart}) VALUES ${valuesPart}`;
        return newQuery;
    }

    static isConnectivityError(err) {
        const code = err && typeof err.code === 'string' && err.code;
        const cls = code && code.substr(0, 2); // Error Class
        return code === 'ECONNRESET' || cls === '08' || cls === '57';
        // Code 'ECONNRESET' - Connectivity issue handled by the driver.
        // Class 08 - Connection Exception.
        // Class 57 - Operator Intervention.
        //
        // ERROR CODES: https://www.postgresql.org/docs/9.6/static/errcodes-appendix.html
    }
}

module.exports = Db;