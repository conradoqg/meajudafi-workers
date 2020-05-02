const pg = require('pg');
const types = require('pg').types;
const dayjs = require('dayjs');
const parse = require('pg-connection-string').parse;
const PgMigrate = require('@urbica/pg-migrate');
const path = require('path');
const prettyMs = require('pretty-ms');
const convertHrtime = require('convert-hrtime');

const CONFIG = require('../config');
const UI = require('./ui');

CONFIG.CONNECTION_STRING = process.env.CONNECTION_STRING ? process.env.CONNECTION_STRING : CONFIG.CONNECTION_STRING;
CONFIG.READONLY_USERNAME = process.env.READONLY_USERNAME ? process.env.READONLY_USERNAME : CONFIG.READONLY_USERNAME;
CONFIG.READONLY_PASSWORD = process.env.READONLY_PASSWORD ? process.env.READONLY_PASSWORD : CONFIG.READONLY_PASSWORD;

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
        GRANT CONNECT ON DATABASE "cvmData" TO readonly;
        GRANT USAGE ON SCHEMA public, private TO readonly;
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public
        	GRANT SELECT ON TABLES TO readonly;
		ALTER DEFAULT PRIVILEGES IN SCHEMA public
            GRANT EXECUTE ON FUNCTIONS TO readonly;	
        ALTER ROLE readonly SET search_path TO public,private;		
        ALTER ROLE readonly IN DATABASE "cvmData" SET default_transaction_isolation TO "read uncommitted";
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
    }

    async takeOffline(refresh = true) {
        refresh && await this.refreshMaterializedViews();        
        return this.pool.end();
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

        const ui = new UI();

        const createTotalProgressInfo = () => {
            return (progress) => `Database: Refreshing materialized view (${progress.view}) (${progress.total}): [${'▇'.repeat((progress.percentage / 2)) + '-'.repeat((100 - progress.percentage) / 2)}] ${progress.percentage.toFixed(2)}% - ${prettyMs(progress.elapsed)} - speed: ${progress.speed.toFixed(2)}v/s - eta: ${Number.isFinite(progress.eta) ? prettyMs(progress.eta) : 'Unknown'}`;
        };

        const createTotalFinishInfo = () => {
            return (progress) => `Database: Refreshing materialized view took ${prettyMs(progress.elapsed)} at ${progress.speed.toFixed(2)}v/s`;
        };

        try {

            const client = await this.pool.connect();
            try {
                const progressState = {
                    total: views.length,
                    view: '',
                    start: process.hrtime(),
                    elapsed: 0,
                    finished: 0,
                    percentage: 0,
                    eta: 0,
                    speed: 0
                };

                ui.start('total', 'Database: Refreshing materialized view', createTotalProgressInfo(), createTotalFinishInfo());
                ui.update('total', progressState);

                for (const view of views) {
                    progressState.view = view;
                    ui.update('total', progressState);

                    await client.query(`REFRESH MATERIALIZED VIEW ${view} WITH DATA;`);

                    progressState.finished++;

                    progressState.elapsed = convertHrtime(process.hrtime(progressState.start)).milliseconds;
                    progressState.speed = progressState.finished / (progressState.elapsed / 100);
                    progressState.eta = ((progressState.elapsed * progressState.total) / progressState.finished) - progressState.elapsed;
                    progressState.percentage = (progressState.finished * 100) / progressState.total;
                    ui.update('total', progressState);
                }

                ui.stop('total');
                ui.close();
            } finally {
                await client.release();
            }            
        } catch (ex) {
            console.error('Error: refreshMaterializedViews');
            console.error(ex.stack);
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
        await pgMigrate.reset();
        await pgMigrate.end();
    }

    createUpsertQuery(data) {
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