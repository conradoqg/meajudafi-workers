const pg = require('pg');
const types = require('pg').types;
const dayjs = require('dayjs');
const DATE = 1082;
const CONFIG = require('../config');

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
        GRANT USAGE ON SCHEMA public TO readonly;
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT ON TABLES TO readonly;

   END IF;
END
$do$;
`;

const extensions = `
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
`;

const f_unaccent = `
CREATE OR REPLACE FUNCTION f_unaccent(text)
  RETURNS text AS
$func$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$func$
LANGUAGE sql
IMMUTABLE;
`;

const inf_cadastral_fi = `
CREATE TABLE IF NOT EXISTS inf_cadastral_fi (
    id UUID NOT NULL UNIQUE,
	CNPJ_FUNDO TEXT NOT NULL,
	DENOM_SOCIAL TEXT,
	DT_REG DATE,
	DT_CONST DATE,
	DT_CANCEL DATE,
	SIT	TEXT,
	DT_INI_SIT DATE,
	DT_INI_ATIV DATE,
	DT_INI_EXERC DATE,
	DT_FIM_EXERC DATE,
	CLASSE TEXT,
	DT_INI_CLASSE DATE,
	RENTAB_FUNDO TEXT,
	CONDOM TEXT,
	FUNDO_COTAS	TEXT,
	FUNDO_EXCLUSIVO	TEXT,
	TRIB_LPRAZO	TEXT,
	INVEST_QUALIF TEXT,
	TAXA_PERFM DOUBLE PRECISION,
	VL_PATRIM_LIQ DOUBLE PRECISION,
	DT_PATRIM_LIQ DATE,
	DIRETOR	TEXT,
	CNPJ_ADMIN TEXT,
	ADMIN TEXT,
	PF_PJ_GESTOR TEXT,
	CPF_CNPJ_GESTOR	TEXT,
	GESTOR TEXT,
	CNPJ_AUDITOR TEXT,
	AUDITOR	TEXT,
	PRIMARY KEY(id, CNPJ_FUNDO)
);
CREATE INDEX IF NOT EXISTS inf_cadastral_fi_cnpj_fundo_index
    ON inf_cadastral_fi USING btree
    (cnpj_fundo ASC);    
`;

const inf_cadastral_fi_fullname = `
CREATE OR REPLACE VIEW inf_cadastral_fi_fullname
AS
SELECT
    id as icf_id,
    CNPJ_FUNDO as icf_CNPJ_FUNDO,
    DENOM_SOCIAL as icf_DENOM_SOCIAL,
    DT_REG as icf_DT_REG,
    DT_CONST as icf_DT_CONST,
    DT_CANCEL as icf_DT_CANCEL,
    SIT as icf_SIT,
    DT_INI_SIT as icf_DT_INI_SIT,
    DT_INI_ATIV as icf_DT_INI_ATIV,
    DT_INI_EXERC as icf_DT_INI_EXERC,
    DT_FIM_EXERC as icf_DT_FIM_EXERC,
    CLASSE as icf_CLASSE,
    DT_INI_CLASSE as icf_DT_INI_CLASSE,
    RENTAB_FUNDO as icf_RENTAB_FUNDO,
    CONDOM as icf_CONDOM,
    FUNDO_COTAS as icf_FUNDO_COTAS,
    FUNDO_EXCLUSIVO as icf_FUNDO_EXCLUSIVO,
    TRIB_LPRAZO as icf_TRIB_LPRAZO,
    INVEST_QUALIF as icf_INVEST_QUALIF,
    TAXA_PERFM as icf_TAXA_PERFM,
    VL_PATRIM_LIQ as icf_VL_PATRIM_LIQ,
    DT_PATRIM_LIQ as icf_DT_PATRIM_LIQ,
    DIRETOR as icf_DIRETOR,
    CNPJ_ADMIN as icf_CNPJ_ADMIN,
    ADMIN as icf_ADMIN,
    PF_PJ_GESTOR as icf_PF_PJ_GESTOR,
    CPF_CNPJ_GESTOR as icf_CPF_CNPJ_GESTOR,
    GESTOR as icf_GESTOR,
    CNPJ_AUDITOR as icf_CNPJ_AUDITOR,
    AUDITOR as icf_AUDITOR
    FROM inf_cadastral_fi
`;

const inf_diario_fi = `
CREATE TABLE IF NOT EXISTS inf_diario_fi (
    id UUID NOT NULL UNIQUE,
	CNPJ_FUNDO TEXT NOT NULL,
	DT_COMPTC DATE NOT NULL,
	VL_TOTAL DOUBLE PRECISION,
	VL_QUOTA DOUBLE PRECISION,
	VL_PATRIM_LIQ DOUBLE PRECISION,
	CAPTC_DIA DOUBLE PRECISION,
	RESG_DIA DOUBLE PRECISION,
	NR_COTST INTEGER,    
    PRIMARY KEY(id, CNPJ_FUNDO, DT_COMPTC),
    CONSTRAINT inf_diario_fi_CNPJ_FUNDO_DT_COMPTC UNIQUE (CNPJ_FUNDO,DT_COMPTC)
);
CREATE INDEX IF NOT EXISTS inf_diario_fi_cnpj_fundo_dt_comptc_index
    ON inf_diario_fi USING btree
    (cnpj_fundo ASC NULLS LAST, dt_comptc ASC NULLS LAST);    
`;

const fbcdata_sgs_12i = `
CREATE TABLE IF NOT EXISTS fbcdata_sgs_12i (
    id UUID NOT NULL UNIQUE,
    DATA DATE NOT NULL,
    VALOR DOUBLE PRECISION,	
    PRIMARY KEY(id, DATA),
    CONSTRAINT fbcdata_sgs_12i_DATA UNIQUE (DATA)
);
CREATE INDEX IF NOT EXISTS fbcdata_sgs_12i_data_index
    ON fbcdata_sgs_12i USING btree
    (DATA ASC NULLS LAST);    
`;

const investment_return_daily = `
CREATE TABLE IF NOT EXISTS investment_return_daily (
    id UUID NOT NULL UNIQUE,
	CNPJ_FUNDO TEXT NOT NULL,
	DT_COMPTC DATE NOT NULL,
    INVESTMENT_RETURN DOUBLE PRECISION,	
    INVESTMENT_RETURN_1Y DOUBLE PRECISION,
    INVESTMENT_RETURN_2Y DOUBLE PRECISION,
    INVESTMENT_RETURN_3Y DOUBLE PRECISION,
    ACCUMULATED_INVESTMENT_RETURN DOUBLE PRECISION,    
    RISK_1Y DOUBLE PRECISION,
    RISK_2Y DOUBLE PRECISION,
    RISK_3Y DOUBLE PRECISION,
    ACCUMULATED_RISK DOUBLE PRECISION,
    SHARPE_1Y DOUBLE PRECISION,
    SHARPE_2Y DOUBLE PRECISION,
    SHARPE_3Y DOUBLE PRECISION,
    ACCUMULATED_SHARPE DOUBLE PRECISION,    
    CONSISTENCY_1Y DOUBLE PRECISION,
    CONSISTENCY_2Y DOUBLE PRECISION,
    CONSISTENCY_3Y DOUBLE PRECISION,
    PRIMARY KEY(id, CNPJ_FUNDO, DT_COMPTC),
    CONSTRAINT investment_return_daily_CNPJ_FUNDO_DT_COMPTC UNIQUE (CNPJ_FUNDO,DT_COMPTC)
);
CREATE INDEX IF NOT EXISTS investment_return_daily_cnpj_fundo_dt_comptc_index
    ON investment_return_daily USING btree
    (cnpj_fundo ASC NULLS LAST, dt_comptc ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS investment_return_daily_dt_comptc_index
    ON investment_return_daily USING btree
    (dt_comptc ASC NULLS LAST);
`;

const investment_return_daily_fullname = `
CREATE OR REPLACE VIEW investment_return_daily_fullname
AS
SELECT
    id as ird_id,
    CNPJ_FUNDO as ird_CNPJ_FUNDO,
    DT_COMPTC as ird_DT_COMPTC,
    INVESTMENT_RETURN as ird_INVESTMENT_RETURN,	
    INVESTMENT_RETURN_1Y as ird_INVESTMENT_RETURN_1Y,
    INVESTMENT_RETURN_2Y as ird_INVESTMENT_RETURN_2Y,
    INVESTMENT_RETURN_3Y as ird_INVESTMENT_RETURN_3Y,
    ACCUMULATED_INVESTMENT_RETURN as ird_ACCUMULATED_INVESTMENT_RETURN, 
    RISK_1Y as ird_RISK_1Y,
    RISK_2Y as ird_RISK_2Y,
    RISK_3Y as ird_RISK_3Y,
    ACCUMULATED_RISK as ird_ACCUMULATED_RISK,
    SHARPE_1Y as ird_SHARPE_1Y,
    SHARPE_2Y as ird_SHARPE_2Y,
    SHARPE_3Y as ird_SHARPE_3Y,
    ACCUMULATED_SHARPE as ird_ACCUMULATED_SHARPE,    
    CONSISTENCY_1Y as ird_CONSISTENCY_1Y,
    CONSISTENCY_2Y as ird_CONSISTENCY_2Y,
    CONSISTENCY_3Y as ird_CONSISTENCY_3Y
    FROM investment_return_daily
`;

const investment_return_monthly = `
CREATE TABLE IF NOT EXISTS investment_return_monthly (
    id UUID NOT NULL UNIQUE,
	CNPJ_FUNDO TEXT NOT NULL,
	DT_COMPTC DATE NOT NULL,
    INVESTMENT_RETURN DOUBLE PRECISION,	
    INVESTMENT_RETURN_1Y DOUBLE PRECISION,
    INVESTMENT_RETURN_2Y DOUBLE PRECISION,
    INVESTMENT_RETURN_3Y DOUBLE PRECISION,
    ACCUMULATED_INVESTMENT_RETURN DOUBLE PRECISION,
    RISK DOUBLE PRECISION,
    RISK_1Y DOUBLE PRECISION,
    RISK_2Y DOUBLE PRECISION,
    RISK_3Y DOUBLE PRECISION,
    ACCUMULATED_RISK DOUBLE PRECISION,
    SHARPE DOUBLE PRECISION,
    SHARPE_1Y DOUBLE PRECISION,
    SHARPE_2Y DOUBLE PRECISION,
    SHARPE_3Y DOUBLE PRECISION,
    ACCUMULATED_SHARPE DOUBLE PRECISION,
    CONSISTENCY_1Y DOUBLE PRECISION,
    CONSISTENCY_2Y DOUBLE PRECISION,
    CONSISTENCY_3Y DOUBLE PRECISION,
    PRIMARY KEY(id, CNPJ_FUNDO, DT_COMPTC),
    CONSTRAINT investment_return_monthly_CNPJ_FUNDO_DT_COMPTC UNIQUE (CNPJ_FUNDO,DT_COMPTC)
);
CREATE INDEX IF NOT EXISTS investment_return_monthly_cnpj_fundo_dt_comptc_index
    ON investment_return_monthly USING btree
    (cnpj_fundo ASC NULLS LAST, dt_comptc ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS investment_return_monthly_dt_comptc_index
    ON investment_return_monthly USING btree
    (dt_comptc ASC NULLS LAST);
`;

const investment_return_monthly_fullname = `
CREATE OR REPLACE VIEW investment_return_monthly_fullname
AS
SELECT
    id as irm_id,
    CNPJ_FUNDO as irm_CNPJ_FUNDO,
    DT_COMPTC as irm_DT_COMPTC,
    INVESTMENT_RETURN as irm_INVESTMENT_RETURN,
    INVESTMENT_RETURN_1Y as irm_INVESTMENT_RETURN_1Y,
    INVESTMENT_RETURN_2Y as irm_INVESTMENT_RETURN_2Y,
    INVESTMENT_RETURN_3Y as irm_INVESTMENT_RETURN_3Y,
    ACCUMULATED_INVESTMENT_RETURN as irm_ACCUMULATED_INVESTMENT_RETURN,
    RISK as irm_RISK,
    RISK_1Y as irm_RISK_1Y,
    RISK_2Y as irm_RISK_2Y,
    RISK_3Y as irm_RISK_3Y,
    ACCUMULATED_RISK as irm_ACCUMULATED_RISK,
    SHARPE as irm_SHARPE,
    SHARPE_1Y as irm_SHARPE_1Y,
    SHARPE_2Y as irm_SHARPE_2Y,
    SHARPE_3Y as irm_SHARPE_3Y,
    ACCUMULATED_SHARPE as irm_ACCUMULATED_SHARPE,
    CONSISTENCY_1Y as irm_CONSISTENCY_1Y,
    CONSISTENCY_2Y as irm_CONSISTENCY_2Y,
    CONSISTENCY_3Y as irm_CONSISTENCY_3Y
    FROM investment_return_monthly
`;

const investment_return_yearly = `
CREATE TABLE IF NOT EXISTS investment_return_yearly (
    id UUID NOT NULL UNIQUE,
	CNPJ_FUNDO TEXT NOT NULL,
	DT_COMPTC DATE NOT NULL,
    INVESTMENT_RETURN DOUBLE PRECISION,	
    INVESTMENT_RETURN_1Y DOUBLE PRECISION,
    INVESTMENT_RETURN_2Y DOUBLE PRECISION,
    INVESTMENT_RETURN_3Y DOUBLE PRECISION,
    ACCUMULATED_INVESTMENT_RETURN DOUBLE PRECISION,
    RISK DOUBLE PRECISION,
    RISK_1Y DOUBLE PRECISION,
    RISK_2Y DOUBLE PRECISION,
    RISK_3Y DOUBLE PRECISION,
    ACCUMULATED_RISK DOUBLE PRECISION,
    SHARPE DOUBLE PRECISION,
    SHARPE_1Y DOUBLE PRECISION,
    SHARPE_2Y DOUBLE PRECISION,
    SHARPE_3Y DOUBLE PRECISION,
    ACCUMULATED_SHARPE DOUBLE PRECISION,
    CONSISTENCY_1Y DOUBLE PRECISION,
    CONSISTENCY_2Y DOUBLE PRECISION,
    CONSISTENCY_3Y DOUBLE PRECISION,
    PRIMARY KEY(id, CNPJ_FUNDO, DT_COMPTC),
    CONSTRAINT investment_return_yearly_CNPJ_FUNDO_DT_COMPTC UNIQUE (CNPJ_FUNDO,DT_COMPTC)
);
CREATE INDEX IF NOT EXISTS investment_return_yearly_cnpj_fundo_dt_comptc_index
    ON investment_return_yearly USING btree
    (cnpj_fundo ASC NULLS LAST, dt_comptc ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS investment_return_yearly_dt_comptc_index
    ON investment_return_yearly USING btree
    (dt_comptc ASC NULLS LAST);
`;

const investment_return_yearly_fullname = `
CREATE OR REPLACE VIEW investment_return_yearly_fullname
AS
SELECT 
    id as iry_id,
    CNPJ_FUNDO as iry_CNPJ_FUNDO,
    DT_COMPTC as iry_DT_COMPTC,
    INVESTMENT_RETURN as iry_INVESTMENT_RETURN,	
    INVESTMENT_RETURN_1Y as iry_INVESTMENT_RETURN_1Y,
    INVESTMENT_RETURN_2Y as iry_INVESTMENT_RETURN_2Y,
    INVESTMENT_RETURN_3Y as iry_INVESTMENT_RETURN_3Y,
    ACCUMULATED_INVESTMENT_RETURN as iry_ACCUMULATED_INVESTMENT_RETURN,
    RISK as iry_RISK,
    RISK_1Y as iry_RISK_1Y,
    RISK_2Y as iry_RISK_2Y,
    RISK_3Y as iry_RISK_3Y,
    ACCUMULATED_RISK as iry_ACCUMULATED_RISK,
    SHARPE as iry_SHARPE,
    SHARPE_1Y as iry_SHARPE_1Y,
    SHARPE_2Y as iry_SHARPE_2Y,
    SHARPE_3Y as iry_SHARPE_3Y,
    ACCUMULATED_SHARPE as iry_ACCUMULATED_SHARPE,
    CONSISTENCY_1Y as iry_CONSISTENCY_1Y,
    CONSISTENCY_2Y as iry_CONSISTENCY_2Y,
    CONSISTENCY_3Y as iry_CONSISTENCY_3Y
    FROM investment_return_yearly
`;

const xpi_funds = `
CREATE TABLE IF NOT EXISTS xpi_funds (
    id UUID NOT NULL UNIQUE,
    CNPJ TEXT NOT NULL UNIQUE,
    XPI_ID TEXT NOT NULL UNIQUE,
    FORMAL_RISK	INTEGER,
    MORNINGSTAR INTEGER,
    NAME TEXT NOT NULL,
    INITIAL_INVESTMENT DOUBLE PRECISION,
    REDEMPTION_DELAY_IN_DAYS TEXT,
    STATE TEXT,
    ADM_FEE DOUBLE PRECISION,
    PERF_FEE DOUBLE PRECISION,
    BENCHMARK TEXT,
    TYPE TEXT,
    PRIMARY KEY(id, CNPJ),
    CONSTRAINT xpi_funds_CNPJ UNIQUE (CNPJ)
);
CREATE INDEX IF NOT EXISTS xpi_funds_CNPJ_index
    ON xpi_funds USING btree
    (CNPJ ASC NULLS LAST);    
`;

const xpi_funds_fullname = `
CREATE OR REPLACE VIEW xpi_funds_fullname
AS
SELECT 
    id as xf_id, 
    CNPJ as xf_CNPJ, 
    XPI_ID as xf_XPI_ID, 
    FORMAL_RISK as xf_FORMAL_RISK, 
    MORNINGSTAR as xf_MORNINGSTAR, 
    NAME as xf_NAME, 
    INITIAL_INVESTMENT as xf_INITIAL_INVESTMENT, 
    REDEMPTION_DELAY_IN_DAYS as xf_REDEMPTION_DELAY_IN_DAYS,
    STATE as xf_STATE,
    ADM_FEE as xf_ADM_FEE,
    PERF_FEE as xf_PERF_FEE,
    BENCHMARK as xf_BENCHMARK,
    TYPE as xf_TYPE
	FROM xpi_funds
`;

const xpi_funds_with_investment_return_monthly_complete_and_expanded = `
DROP MATERIALIZED VIEW IF EXISTS xpi_funds_with_investment_return_monthly_complete_and_expanded_materialized;
DROP VIEW IF EXISTS xpi_funds_with_investment_return_monthly_complete_and_expanded;
CREATE OR REPLACE VIEW xpi_funds_with_investment_return_monthly_complete_and_expanded
AS 
SELECT 
	xf_name, 
	icf_classe, 
	last_day_of_month as irm_dt_comptc, 
	COALESCE(irm_accumulated_investment_return,0) as irm_accumulated_investment_return, 
	COALESCE(irm_accumulated_risk,0) as irm_accumulated_risk 
	FROM (SELECT * FROM xpi_funds_fullname
			LEFT JOIN inf_cadastral_fi_fullname ON xf_cnpj = icf_cnpj_fundo
			CROSS JOIN (SELECT CAST(date_trunc('month',ts) + interval '1 month - 1 day' AS DATE) AS last_day_of_month
			FROM generate_series(
				(SELECT MIN(irm_dt_comptc)::timestamp::date FROM investment_return_monthly_fullname),
				(SELECT MAX(irm_dt_comptc)::timestamp::date FROM investment_return_monthly_fullname),
				interval '1 month') as ts) REFERENCE_DATE_SERIES) AS DATE_SERIES	
	LEFT JOIN investment_return_monthly_fullname ON irm_cnpj_fundo = DATE_SERIES.xf_cnpj AND DATE_SERIES.last_day_of_month = irm_dt_comptc				
	ORDER BY irm_dt_comptc, xf_name, icf_classe
`;

const xpi_funds_with_investment_return_monthly_complete_and_expanded_materialized = `
CREATE MATERIALIZED VIEW xpi_funds_with_investment_return_monthly_complete_and_expanded_materialized
AS
SELECT * FROM xpi_funds_with_investment_return_monthly_complete_and_expanded
WITH DATA
`;

const inf_cadastral_fi_with_xpi_and_iryf_of_last_year = `
DROP INDEX IF EXISTS inf_cadastral_fi_with_xpi_and_iryf_of_last_year_f_unaccent_idx;
DROP FUNCTION IF EXISTS icf_denom_social_unaccented;
DROP MATERIALIZED VIEW IF EXISTS inf_cadastral_fi_with_xpi_and_iryf_of_last_year;
CREATE MATERIALIZED VIEW inf_cadastral_fi_with_xpi_and_iryf_of_last_year
AS
SELECT DISTINCT ON (icf_CNPJ_FUNDO) icf_CNPJ_FUNDO as DISTINCT_icf_CNPJ_FUNDO,* 
	FROM inf_cadastral_fi_fullname 
	LEFT JOIN xpi_funds_fullname ON inf_cadastral_fi_fullname.icf_cnpj_fundo = xpi_funds_fullname.xf_cnpj
	LEFT JOIN (
		SELECT DISTINCT ON (iry_cnpj_fundo) iry_cnpj_fundo as DISTINCT_iry_cnpj_fundo, * FROM investment_return_yearly_fullname ORDER BY iry_cnpj_fundo, iry_dt_comptc DESC
	) AS LAST_YEAR ON inf_cadastral_fi_fullname.icf_cnpj_fundo = LAST_YEAR.iry_cnpj_fundo
WITH DATA;
CREATE INDEX IF NOT EXISTS inf_cadastral_fi_with_xpi_and_iryf_of_last_year_f_unaccent_idx ON inf_cadastral_fi_with_xpi_and_iryf_of_last_year USING gin(f_unaccent(icf_denom_social) gin_trgm_ops);
CREATE OR REPLACE FUNCTION icf_denom_social_unaccented(inf_cadastral_fi_with_xpi_and_iryf_of_last_year) RETURNS text AS $$
  SELECT f_unaccent($1.icf_denom_social);
$$ LANGUAGE SQL;
`;

const refresh_xpi_funds_with_investment_return_monthly_complete_and_expanded_materialized = `
REFRESH MATERIALIZED VIEW xpi_funds_with_investment_return_monthly_complete_and_expanded_materialized;
`;

const refresh_inf_cadastral_fi_with_xpi_and_iryf_of_last_year = `
REFRESH MATERIALIZED VIEW inf_cadastral_fi_with_xpi_and_iryf_of_last_year;
`;

class Db {
    constructor() {
        this.pool = new pg.Pool({
            connectionString: CONFIG.CONNECTION_STRING,
            max: CONFIG.POOL_SIZE,
            connectionTimeoutMillis: 0,
            idleTimeoutMillis: 0
        });
        this.pool.on('error', (err) => {
            console.error('An idle client has experienced an error\n', err.stack)
        });
    }

    async takeOnline() {
        return this.migrate();
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
        const client = await this.pool.connect();
        try {
            await client.query(createReadOnlyUser);
            await client.query(extensions);
            await client.query(f_unaccent);
            await client.query(inf_cadastral_fi);
            await client.query(inf_cadastral_fi_fullname);
            await client.query(inf_diario_fi);
            await client.query(fbcdata_sgs_12i);
            await client.query(investment_return_daily);
            await client.query(investment_return_daily_fullname);
            await client.query(investment_return_monthly);
            await client.query(investment_return_monthly_fullname);
            await client.query(investment_return_yearly);
            await client.query(investment_return_yearly_fullname);
            await client.query(xpi_funds);
            await client.query(xpi_funds_fullname);
            await client.query(xpi_funds_with_investment_return_monthly_complete_and_expanded);
            await client.query(xpi_funds_with_investment_return_monthly_complete_and_expanded_materialized);
            await client.query(inf_cadastral_fi_with_xpi_and_iryf_of_last_year);
        } catch (ex) {
            throw ex;
        } finally {
            client.release();
        }
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