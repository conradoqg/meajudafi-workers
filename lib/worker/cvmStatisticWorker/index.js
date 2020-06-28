const promisePipe = require('promisepipe');
const stream = require('stream');

const PooledWorker = require('../pooledWorker');
const CONFIG = require('../../util/config');
const createFundCalculatorStream = require('../../stream/createFundCalculatorStream');
const createWaitForPromisesStream = require('../../stream/createWaitForPromisesStream');

class CVMStatisticWorker extends PooledWorker {
    id = 'CVMStatisticWorker'

    constructor() {
        super();
    }

    async getPendingFunds(pool, progress, options) {
        progress.start();

        let full = false;
        let interestingFundsOnly = false;
        let onlyOne = false;
        if (options.options && options.options.includes('fullStatistics')) full = true;
        if (options.options && options.options.includes('interestingFundsOnly')) interestingFundsOnly = true;
        if (options.options && options.options.includes('onlyOne')) onlyOne = true;

        let isClientErrored = false;
        const mainClient = await pool.connect();
        mainClient.on('error', err => {
            progress.log(`Database client errored: ${err.stack}`);
            isClientErrored = true;
        });

        let query = null;

        if (full) query = 'SELECT cnpj_fundo FROM inf_diario_fi GROUP BY cnpj_fundo';
        else if (interestingFundsOnly) query = 'SELECT cnpj_fundo FROM inf_diario_fi JOIN funds_enhanced ON inf_diario_fi.cnpj_fundo = funds_enhanced.f_cnpj WHERE xf_id IS NOT NULL OR bf_id IS NOT NULL OR mf_id IS NOT NULL GROUP BY cnpj_fundo';
        else if (onlyOne) query = 'SELECT cnpj_fundo FROM inf_diario_fi JOIN funds_enhanced ON inf_diario_fi.cnpj_fundo = funds_enhanced.f_cnpj WHERE (xf_id IS NOT NULL OR bf_id IS NOT NULL OR mf_id IS NOT NULL) AND cnpj_fundo = \'22232927000190\' GROUP BY cnpj_fundo';
        else query = 'SELECT cnpj_fundo, MIN(DT_COMPTC) as start FROM inf_diario_fi WHERE pending_statistic_at is not null GROUP BY cnpj_fundo';

        try {

            const queryfundsCount = await mainClient.query(query);
            const fundsCount = parseInt(queryfundsCount.rows.length);

            return { queryfundsCount, fundsCount };
        } catch (ex) {
            throw new Error(`Query "${query}" failed`, ex);
        } finally {
            if (!isClientErrored) await mainClient.release();
        }
    }

    async calculateStatistics(pool, progress, { queryfundsCount, fundsCount }) {
        progress.start(fundsCount);

        const updateUIProgressStream = stream.Transform({
            objectMode: true,
            highWaterMark: CONFIG.highWaterMark,
            transform: (chunk, e, callback) => {
                progress.step();
                callback(null, chunk);
            }
        });

        const fundsStream = new stream.Readable({
            objectMode: true,
            highWaterMark: CONFIG.highWaterMark
        });

        const statisticsPipe = promisePipe(
            fundsStream,
            createFundCalculatorStream(pool),
            updateUIProgressStream,
            createWaitForPromisesStream()
        );

        queryfundsCount.rows.forEach(item => fundsStream.push(item));
        fundsStream.push(null);

        try {
            return await statisticsPipe;
        } catch (ex) {
            throw ex.originalError;
        }
    }
}

module.exports = CVMStatisticWorker;