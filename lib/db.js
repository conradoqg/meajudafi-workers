const pg = require('pg');
const types = require('pg').types;
const moment = require('moment');
const DATE = 1082;

types.setTypeParser(DATE, val => val === null ? null : moment(val));

const inf_cadastral_fi = `
CREATE TABLE IF NOT EXISTS inf_cadastral_fi (
    id    UUID NOT NULL UNIQUE,
	CNPJ_FUNDO	TEXT NOT NULL,
	DENOM_SOCIAL	TEXT,
	DT_REG	DATE,
	DT_CONST	DATE,
	DT_CANCEL	DATE,
	SIT	TEXT,
	DT_INI_SIT	DATE,
	DT_INI_ATIV	DATE,
	DT_INI_EXERC	DATE,
	DT_FIM_EXERC	DATE,
	CLASSE	TEXT,
	DT_INI_CLASSE	DATE,
	RENTAB_FUNDO	TEXT,
	CONDOM	TEXT,
	FUNDO_COTAS	TEXT,
	FUNDO_EXCLUSIVO	TEXT,
	TRIB_LPRAZO	TEXT,
	INVEST_QUALIF	TEXT,
	TAXA_PERFM	DOUBLE PRECISION,
	VL_PATRIM_LIQ	DOUBLE PRECISION,
	DT_PATRIM_LIQ	DATE,
	DIRETOR	TEXT,
	CNPJ_ADMIN	TEXT,
	ADMIN	TEXT,
	PF_PJ_GESTOR	TEXT,
	CPF_CNPJ_GESTOR	TEXT,
	GESTOR	TEXT,
	CNPJ_AUDITOR	TEXT,
	AUDITOR	TEXT,
	PRIMARY KEY(id, CNPJ_FUNDO)	
);
CREATE INDEX IF NOT EXISTS inf_cadastral_fi_cnpj_fundo_index
    ON inf_cadastral_fi USING btree
    (cnpj_fundo ASC);    
`;

const inf_diario_fi = `
CREATE TABLE IF NOT EXISTS inf_diario_fi (
    id    UUID NOT NULL UNIQUE,
	CNPJ_FUNDO	TEXT NOT NULL,
	DT_COMPTC	DATE NOT NULL,
	VL_TOTAL	DOUBLE PRECISION,
	VL_QUOTA	DOUBLE PRECISION,
	VL_PATRIM_LIQ	DOUBLE PRECISION,
	CAPTC_DIA	DOUBLE PRECISION,
	RESG_DIA	DOUBLE PRECISION,
	NR_COTST	INTEGER,    
    PRIMARY KEY(id, CNPJ_FUNDO, DT_COMPTC),
    CONSTRAINT inf_diario_fi_CNPJ_FUNDO_DT_COMPTC UNIQUE (CNPJ_FUNDO,DT_COMPTC)
);
CREATE INDEX IF NOT EXISTS inf_diario_fi_cnpj_fundo_dt_comptc_index
    ON inf_diario_fi USING btree
    (cnpj_fundo ASC NULLS LAST, dt_comptc ASC NULLS LAST);    
`;

const investment_return_daily = `
CREATE TABLE IF NOT EXISTS investment_return_daily (
    id    UUID NOT NULL UNIQUE,
	CNPJ_FUNDO	TEXT NOT NULL,
	DT_COMPTC	DATE NOT NULL,
    RETURN	DOUBLE PRECISION,	
    ACCUMULATED_TOTAL_RETURN DOUBLE PRECISION,
    PRIMARY KEY(id, CNPJ_FUNDO, DT_COMPTC),
    CONSTRAINT investment_return_daily_CNPJ_FUNDO_DT_COMPTC UNIQUE (CNPJ_FUNDO,DT_COMPTC)
);
CREATE INDEX IF NOT EXISTS investment_return_daily_cnpj_fundo_dt_comptc_index
    ON investment_return_daily USING btree
    (cnpj_fundo ASC NULLS LAST, dt_comptc ASC NULLS LAST);
`;

const investment_return_monthly = `
CREATE TABLE IF NOT EXISTS investment_return_monthly (
    id    UUID NOT NULL UNIQUE,
	CNPJ_FUNDO	TEXT NOT NULL,
	DT_COMPTC	DATE NOT NULL,
    RETURN	DOUBLE PRECISION,	
    ACCUMULATED_TOTAL_RETURN DOUBLE PRECISION,
    MONTHS_RISK DOUBLE PRECISION,
    ACCUMULATED_TOTAL_RISK DOUBLE PRECISION,
    PRIMARY KEY(id, CNPJ_FUNDO, DT_COMPTC),
    CONSTRAINT investment_return_monthly_CNPJ_FUNDO_DT_COMPTC UNIQUE (CNPJ_FUNDO,DT_COMPTC)
);
CREATE INDEX IF NOT EXISTS investment_return_monthly_cnpj_fundo_dt_comptc_index
    ON investment_return_monthly USING btree
    (cnpj_fundo ASC NULLS LAST, dt_comptc ASC NULLS LAST);
`;

const investment_return_yearly = `
CREATE TABLE IF NOT EXISTS investment_return_yearly (
    id    UUID NOT NULL UNIQUE,
	CNPJ_FUNDO	TEXT NOT NULL,
	DT_COMPTC	DATE NOT NULL,
    RETURN	DOUBLE PRECISION,	
    ACCUMULATED_TOTAL_RETURN DOUBLE PRECISION,
    YEARS_RISK DOUBLE PRECISION,
    ACCUMULATED_TOTAL_RISK DOUBLE PRECISION,
    PRIMARY KEY(id, CNPJ_FUNDO, DT_COMPTC),
    CONSTRAINT investment_return_yearly_CNPJ_FUNDO_DT_COMPTC UNIQUE (CNPJ_FUNDO,DT_COMPTC)
);
CREATE INDEX IF NOT EXISTS investment_return_yearly_cnpj_fundo_dt_comptc_index
    ON investment_return_yearly USING btree
    (cnpj_fundo ASC NULLS LAST, dt_comptc ASC NULLS LAST);
`;

const CONFIG = {
    CONNECTION_STRING: 'postgresql://postgres:temporary@pgadmin.conradoqg.eti.br:5432/cvmData',
    POOL_SIZE: 20
};

class Db {
    constructor() {
        this.pool = new pg.Pool({
            connectionString: CONFIG.CONNECTION_STRING,
            max: CONFIG.POOL_SIZE
        });
    }

    async takeOnline() {
        return this.migrate();
    }

    async takeOffline() {
        return this.pool.end();
    }

    async migrate() {
        const client = await this.pool.connect();
        try {
            await client.query(inf_cadastral_fi);
            await client.query(inf_diario_fi);
            await client.query(investment_return_daily);
            await client.query(investment_return_monthly);
            await client.query(investment_return_yearly);
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