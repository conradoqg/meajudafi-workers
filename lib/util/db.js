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

types.setTypeParser(DATE, val => val === null ? null : dayjs(val));

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

const refresh_xpi_funds_with_investment_return_monthly_complete_and_expanded_materialized = `
REFRESH MATERIALIZED VIEW xpi_funds_with_investment_return_monthly_complete_and_expanded_materialized;
`;

const refresh_inf_cadastral_fi_with_xpi_and_iryf_of_last_year = `
REFRESH MATERIALIZED VIEW inf_cadastral_fi_with_xpi_and_iryf_of_last_year;
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
            await client.query(refresh_xpi_funds_with_investment_return_monthly_complete_and_expanded_materialized);
            await client.query(refresh_inf_cadastral_fi_with_xpi_and_iryf_of_last_year);

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
        const values = data.values.map(values => `(${Object.values(values).map(value => (value != null ? `'${value}'` : 'null')).join(', ')})`).join(',');
        const fields = Object.keys(data.values[0]).join(', ');
        const onConflict = (Array.isArray(data.primaryKey) ? data.primaryKey.join(', ') : data.primaryKey);
        const updateFields = Object.keys(data.values[0]).slice(0).map(value => `${value} = excluded.${value}`).join(', ');
        let newQuery = null;
        if (data.primaryKey)
            newQuery = `INSERT INTO ${data.table} (${fields}) VALUES ${values} ON CONFLICT (${onConflict}) DO UPDATE SET ${updateFields}`;
        else
            newQuery = `INSERT INTO ${data.table} (${fields}) VALUES ${values}`;
        return newQuery;
    }

}

module.exports = Db;