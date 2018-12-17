const stream = require('stream');
const QueryStream = require('pg-query-stream');
const promisePipe = require('promisepipe');

const createAccumulatorStream = require('./createAccumulatorStream');
const createInsertPromiseStream = require('./createInsertPromiseStream');
const createCalculatorStream = require('./createCalculatorStream');
const createWaitForPromisesStream = require('./createWaitForPromisesStream');
const CONFIG = require('../config');

const createFundCalculatorStream = (db) => {
    return stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: async (chunk, e, callback) => {
            try {
                const client = await db.pool.connect();
                try {
                    await client.query('BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');
                    const query = new QueryStream(`
                    SELECT *, 
                        fbcdata_sgs_1i.data as dolar_data, fbcdata_sgs_1i.valor as dolar_valor,
                        fbcdata_sgs_7i.data as bovespa_data, fbcdata_sgs_7i.valor as bovespa_valor,
                        fbcdata_sgs_11i.data as selic_data, fbcdata_sgs_11i.valor as selic_valor,
                        fbcdata_sgs_12i.data as cdi_data, fbcdata_sgs_12i.valor as cdi_valor,
                        fbcdata_sgs_189i.data as igpm_data, fbcdata_sgs_189i.valor as igpm_valor,
                        fbcdata_sgs_190i.data as igpdi_data, fbcdata_sgs_190i.valor as igpdi_valor,
                        fbcdata_sgs_433i.data as ipca_data, fbcdata_sgs_433i.valor as ipca_valor,
                        fbcdata_sgs_21619i.data as euro_data, fbcdata_sgs_21619i.valor as euro_valor
                        FROM inf_diario_fi 	
                            LEFT JOIN fbcdata_sgs_1i ON inf_diario_fi.DT_COMPTC = fbcdata_sgs_1i.DATA
                            LEFT JOIN fbcdata_sgs_7i ON inf_diario_fi.DT_COMPTC = fbcdata_sgs_7i.DATA
                            LEFT JOIN fbcdata_sgs_11i ON inf_diario_fi.DT_COMPTC = fbcdata_sgs_11i.DATA
                            LEFT JOIN fbcdata_sgs_12i ON inf_diario_fi.DT_COMPTC = fbcdata_sgs_12i.DATA
                            LEFT JOIN fbcdata_sgs_189i ON date_part('year', inf_diario_fi.DT_COMPTC) = date_part('year', fbcdata_sgs_189i.DATA) AND date_part('month', inf_diario_fi.DT_COMPTC) = date_part('month', fbcdata_sgs_189i.DATA)
                            LEFT JOIN fbcdata_sgs_190i ON date_part('year', inf_diario_fi.DT_COMPTC) = date_part('year', fbcdata_sgs_190i.DATA) AND date_part('month', inf_diario_fi.DT_COMPTC) = date_part('month', fbcdata_sgs_190i.DATA)
                            LEFT JOIN fbcdata_sgs_433i ON date_part('year', inf_diario_fi.DT_COMPTC) = date_part('year', fbcdata_sgs_433i.DATA) AND date_part('month', inf_diario_fi.DT_COMPTC) = date_part('month', fbcdata_sgs_433i.DATA)
                            LEFT JOIN fbcdata_sgs_21619i ON inf_diario_fi.DT_COMPTC = fbcdata_sgs_21619i.DATA
                        WHERE cnpj_fundo = '${chunk.cnpj_fundo}' ORDER BY DT_COMPTC ASC
                    `);
                    const dataStream = client.query(query);

                    const fundPromise = promisePipe(
                        dataStream,
                        createCalculatorStream(),
                        createAccumulatorStream(),
                        createInsertPromiseStream(db),
                        createWaitForPromisesStream()
                    );

                    callback(null, fundPromise);
                    await fundPromise;

                    // Force set to null pending_statistic_at
                    await client.query(`UPDATE inf_diario_fi SET pending_statistic_at = '0001-01-01' WHERE cnpj_fundo = '${chunk.cnpj_fundo}' AND pending_statistic_at IS NOT NULL`);

                    await client.query('COMMIT');
                } catch (ex) {
                    await client.query('ROLLBACK');
                    throw ex;
                } finally {
                    client.release();
                }
            } catch (ex) {
                console.error(ex);
                callback(ex);
            }
        }
    });
};

module.exports = createFundCalculatorStream;