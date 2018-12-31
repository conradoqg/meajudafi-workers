const pg = require('pg');
const types = require('pg').types;
const dayjs = require('dayjs');
const DATE = 1082;
const CONFIG = require('../config');
const parse = require('pg-connection-string').parse;
const PgMigrate = require('@urbica/pg-migrate');
const path = require('path');

CONFIG.CONNECTION_STRING = process.env.CONNECTION_STRING ? process.env.CONNECTION_STRING : CONFIG.CONNECTION_STRING;
CONFIG.READONLY_USERNAME = process.env.READONLY_USERNAME ? process.env.READONLY_USERNAME : CONFIG.READONLY_USERNAME;
CONFIG.READONLY_PASSWORD = process.env.READONLY_PASSWORD ? process.env.READONLY_PASSWORD : CONFIG.READONLY_PASSWORD;

types.setTypeParser(DATE, val => val === null ? null : dayjs(new Date(parseInt(val.substring(0, 4)), parseInt(val.substring(5, 7)) - 1, parseInt(val.substring(8, 10)))));

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

   END IF;
END
$do$;
`;

const refresh_icf_with_xf_and_iry_of_last_year = `
REFRESH MATERIALIZED VIEW icf_with_xf_and_iry_of_last_year;
`;

const refresh_xf_with_irm_timeseries = `
REFRESH MATERIALIZED VIEW xf_with_irm_timeseries;
`;

const refresh_running_days_with_indicators = `
REFRESH MATERIALIZED VIEW running_days_with_indicators;
`;

const refresh_running_days = `
REFRESH MATERIALIZED VIEW running_days;
`;

class Db {
    async takeOnline() {
        this.pool = new pg.Pool({
            connectionString: CONFIG.CONNECTION_STRING,
            max: CONFIG.POOL_SIZE
        });
        this.pool.on('error', (err) => {
            console.error('An idle client has experienced an error\n', err.stack);
        });

        const client = await this.pool.connect();
        try {
            await client.query(createReadOnlyUser);
        } catch (ex) {
            throw ex;
        } finally {
            client.release();
        }
    }

    async takeOffline() {
        const client = await this.pool.connect();
        try {
            console.log('Refreshing icf_with_xf_and_iry_of_last_year');
            await client.query(refresh_icf_with_xf_and_iry_of_last_year);
            console.log('Refreshing xf_with_irm_timeseries');
            await client.query(refresh_xf_with_irm_timeseries);
            console.log('Refreshing running_days');
            await client.query(refresh_running_days);
            console.log('Refreshing running_days_with_indicators');
            await client.query(refresh_running_days_with_indicators);
            console.log('Materialized views refreshed');
        } catch (ex) {
            throw ex;
        } finally {
            client.release();
        }
        return this.pool.end();
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
                if (!cache[data.table]) {
                    cache[data.table] = {
                        fieldsPart: Object.keys(data.values[0]).join(', '),
                        updateFieldsPart: Object.keys(data.values[0]).slice(0).map(value => `${value} = excluded.${value}`).join(', ')
                    };
                }
                fieldsPart = cache[data.table].fieldsPart;
                updateFieldsPart = cache[data.table].updateFieldsPart;
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
}

let cache = {};

module.exports = Db;