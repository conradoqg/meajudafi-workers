const pg = require('pg');

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
	TAXA_PERFM	REAL,
	VL_PATRIM_LIQ	REAL,
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
`;

const inf_diario_fi = `
CREATE TABLE IF NOT EXISTS inf_diario_fi (
    id    UUID NOT NULL UNIQUE,
	CNPJ_FUNDO	TEXT NOT NULL,
	DT_COMPTC	DATE NOT NULL,
	VL_TOTAL	REAL,
	VL_QUOTA	REAL,
	VL_PATRIM_LIQ	REAL,
	CAPTC_DIA	REAL,
	RESG_DIA	REAL,
	NR_COTST	INTEGER,    
    PRIMARY KEY(id, CNPJ_FUNDO, DT_COMPTC),
    CONSTRAINT ux_CNPJ_FUNDO_DT_COMPTC UNIQUE (CNPJ_FUNDO,DT_COMPTC)
);
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
        } catch (ex) {
            throw ex;
        } finally {
            client.end();
        }
    }
}

module.exports = Db;