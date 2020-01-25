const Db = require('../../util/db');
const prettyMs = require('pretty-ms');
const convertHrtime = require('convert-hrtime');
const got = require('got');
const stringSimilarity = require('string-similarity');
const allKeys = require('promise-results/allKeys');

const Worker = require('../worker');
const UI = require('../../util/ui');
const formatters = require('../../util/formatters');

const createProcessingProgressInfo = () => {
    return (progress) => `BTGPactualFundWorker: Processing data from BTG of ${progress.total} funds: [${'▇'.repeat(progress.percentage / 2) + '-'.repeat(100 / 2 - progress.percentage / 2)}] ${progress.percentage.toFixed(2)}% - ${prettyMs(progress.elapsed)} - speed: ${progress.speed.toFixed(2)}r/s - eta: ${Number.isFinite(progress.eta) ? prettyMs(progress.eta) : 'Unknown'}`;
};

const createProcessingFinishInfo = () => {
    return (progress) => `BTGPactualFundWorker: Processing data took ${prettyMs(progress.elapsed)} at ${progress.speed.toFixed(2)}r/s and found ${((progress.found * 100) / progress.total).toFixed(2)}%`;
};

class BTGPactualFundWorker extends Worker {
    constructor() {
        super();
    }

    async work() {
        try {

            const db = new Db();

            await db.takeOnline();

            try {
                const mainClient = await db.pool.connect();

                try {
                    const ui = new UI();

                    await mainClient.query('BEGIN TRANSACTION');

                    ui.start('BTGPactualFundWorkerData', 'BTGPactualFundWorker: Getting data from BTG', null, progress => `BTGPactualFundWorker: Getting data from BTG took ${prettyMs(progress.elapsed)}`, true);
                    const startData = process.hrtime();

                    const { funds, btgFunds } = await getFundsData(mainClient);

                    ui.update('BTGPactualFundWorkerData', { elapsed: convertHrtime(process.hrtime(startData)).milliseconds });
                    ui.stop('BTGPactualFundWorkerData');

                    ui.start('BTGPactualFundWorkerDataProcessing', 'BTGPactualFundWorker: Processing data', createProcessingProgressInfo(), createProcessingFinishInfo());

                    const rowsToUpsert = discoverFunds(funds, btgFunds, (progress) => ui.update('BTGPactualFundWorkerDataProcessing', progress));

                    ui.stop('BTGPactualFundWorkerDataProcessing');

                    ui.start('BTGPactualFundWorkerUpdate', 'BTGPactualFundWorker: Updating database', null, progress => `BTGPactualFundWorker: Updating database took ${prettyMs(progress.elapsed)}`, true);
                    const startUpdate = process.hrtime();

                    let newQuery = db.createUpsertQuery({
                        table: 'btgpactual_funds',
                        primaryKey: 'bf_id',
                        values: rowsToUpsert
                    });

                    await mainClient.query({
                        text: newQuery,
                        rowMode: 'array'
                    });

                    ui.update('BTGPactualFundWorkerUpdate', { elapsed: convertHrtime(process.hrtime(startUpdate)).milliseconds });
                    ui.stop('BTGPactualFundWorkerUpdate');

                    ui.close();

                    await mainClient.query('COMMIT');
                } catch (ex) {
                    await mainClient.query('ROLLBACK');
                    throw ex;
                } finally {
                    mainClient.release();
                }
            } finally {
                await db.takeOffline();
            }
        } catch (ex) {
            console.error('Error: BTGPactualFundWorker: work');
            console.error(ex.stack);
        }
    }
}

const getFundsData = async (pool) => {
    return allKeys({
        funds: pool.query('SELECT f_cnpj, f_short_name FROM funds'),
        btgFunds: got.get('https://www.btgpactualdigital.com/services/public/funds/').then(result => JSON.parse(result.body))
    });
};

const discoverFunds = (funds, btgFunds, progressCallback) => {
    const progressState = {
        total: btgFunds.length,
        start: process.hrtime(),
        found: 0,
        elapsed: 0,
        finished: 0,
        percentage: 0,
        eta: 0,
        speed: 0
    };

    const reportProgress = () => {
        progressState.elapsed = convertHrtime(process.hrtime(progressState.start)).milliseconds;
        progressState.speed = progressState.finished / (progressState.elapsed / 100);
        progressState.eta = ((progressState.elapsed * progressState.total) / progressState.finished) - progressState.elapsed;
        progressState.percentage = (progressState.finished * 100) / progressState.total;
        progressCallback(progressState);
    };

    reportProgress();

    const normalize = (name) => {
        return name.toLowerCase()
            .replace(/[àáâãäå]/gi, 'a')
            .replace(/æ/gi, 'ae')
            .replace(/ç/gi, 'c')
            .replace(/[èéêë]/gi, 'e')
            .replace(/[ìíîï]/gi, 'i')
            .replace(/ñ/gi, 'n')
            .replace(/[òóôõö]/gi, 'o')
            .replace(/œ/gi, 'oe')
            .replace(/[ùúûü]/gi, 'u')
            .replace(/[ýÿ]/gi, 'y')
            .replace(/(^| )(ficfim)( |$)/ig, '$1fic fim$3')
            .replace(/(^| )(ficfi)( |$)/ig, '$1fic fi$3')
            .replace(/(^| )(equity)( |$)/ig, '$1eq$3')
            .replace(/(^| )(equities)( |$)/ig, '$1eq$3')
            .replace(/(^| )(absoluto)( |$)/ig, '$1abs$3')
            .replace(/(^| )(fi cotas)( |$)/ig, '$1fic$3')
            .replace(/(^| )(fi acoes)( |$)/ig, '$1fia$3')
            .replace(/(^| )(cred priv)( |$)/ig, '$1cp$3')
            .replace(/(^| )(credito privado)( |$)/ig, '$1cp$3')
            .replace(/(^| )(cred corp)( |$)/ig, '$1cp$3')
            .replace(/(^| )(cred pri)( |$)/ig, '$1cp$3')
            .replace(/(^| )(deb in)( |$)/ig, '$1debenture incentivadas$3')
            .replace(/(^| )(deb inc)( |$)/ig, '$1debenture incentivadas$3')
            .replace(/(^| )(deb incent)( |$)/ig, '$1debenture incentivadas$3')
            .replace(/(^| )(deb incentivadas)( |$)/ig, '$1debenture incentivadas$3')
            .replace(/(^| )(infra incentivado)( |$)/ig, '$1debenture incentivadas$3')
            .replace(/(^| )(mult)( |$)/ig, '$1multistrategy$3')
            .replace(/(^| )(cap)( |$)/ig, '$1capital$3')
            .replace(/(^| )(small)( |$)/ig, '$1s$3')
            .replace(/(^| )(glob)( |$)/ig, '$1global$3')
            .replace(/(^| )(g)( |$)/ig, '$1global$3')
            .replace(/(^| )(de)( |$)/ig, ' ')
            .replace(/(^| )(acs)( |$)/ig, '$1access$3')
            .replace(/(^| )(cr)( |$)/ig, '$1credit$3')
            .replace(/(^| )(ref)( |$)/ig, '$1referenciado$3')
            .replace(/(^| )(fiq)( |$)/ig, '$1fic$3')
            .replace(/(^| )(fi multimercado)( |$)/ig, '$1fim$3')
            .replace(/(^| )(fi multimercad)( |$)/ig, '$1fim$3')
            .replace(/(^| )(fi multistrategy)( |$)/ig, '$1fim$3')
            .replace(/(^| )(lb)( |$)/ig, '$1long biased$3')
            .replace(/(^| )(ls)( |$)/ig, '$1long short$3')
            .replace(/(^| )(dl)( |$)/ig, '$1direct lending$3')
            .replace(/(^| )(fundo (?:de )?investimento)( |$)/ig, '$1fi$3')
            .replace(/(^| )(btgp)( |$)/ig, '$1btg pactual$3')
            .replace(/(^| )(btg pac)( |$)/ig, '$1btg pactual$3')
            .replace(/(^| )(credito priv)( |$)/ig, '$1cp$3')
            .replace(/(^| )(fi renda fixa)( |$)/ig, '$1firf$3')
            .replace(/(^| )(renda fixa)( |$)/ig, '$1rf$3');
    };

    const avaliableFundsName = [];
    const avaliableFundsNameIndex = [];

    funds.rows.map((row, index) => {
        const normalizedName = normalize(row.f_short_name);
        avaliableFundsName.push(normalizedName);
        avaliableFundsNameIndex[normalizedName] = index;
    });

    let rowsToUpsert = [];

    btgFunds.forEach(item => {
        const row = {            
            bf_id: item.id,
            bf_cnpj: null,
            bf_date: (new Date()).toISOString(),
            bf_product: item.product.replace(/'/g, ''),
            bf_description: item.description.replace(/\n/g, ''),
            bf_type: item.type,
            bf_risk_level: item.riskLevel,
            bf_risk_name: item.riskName,
            bf_minimum_initial_investment: item.minimumInitialInvestment,
            bf_investor_type: item.tipoInvestimentoIndicador,
            bf_minimum_moviment: item.detail.minimumMoviment,
            bf_minimum_balance_remain: item.detail.minimumBalanceRemain,
            bf_administration_fee: item.detail.administrationFee / 100,
            bf_performance_fee: item.detail.performanceFee / 100,
            bf_number_of_days_financial_investment: item.detail.numberOfDaysFinancialInvestment,
            bf_investment_quota: formatters.removeRelativeBRDate(item.detail.investimentQuota),
            bf_investment_financial_settlement: formatters.removeRelativeBRDate(item.detail.investmentFinancialSettlement),
            bf_rescue_quota: formatters.removeRelativeBRDate(item.detail.rescueQuota),
            bf_rescue_financial_settlement: formatters.removeRelativeBRDate(item.detail.rescuefinancialSettlement),
            bf_anbima_rating: item.detail.anbimaRating,
            bf_anbima_code: item.detail.anbimaCode,
            bf_category_description: item.detail.categoryDescription,
            bf_category_code: item.detail.categoryCode,
            bf_custody: item.detail.custody,
            bf_auditing: item.detail.auditing,
            bf_manager: item.detail.manager,
            bf_administrator: item.detail.administrator,
            bf_quotaRule: item.detail.quotaRule,
            bf_net_equity: item.netEquity,
            bf_inactive: item.inAtivo,
            bf_issuer_name: item.issuerName,
            bf_external_issuer: item.externalIssuer,
            bf_is_recent_fund: item.isRecentFund,
            bf_is_blacklist: item.isBlackList,
            bf_is_whitelist: item.isWhiteList
        };

        const normalizedProduct = normalize(item.product);
        const bestMatch = stringSimilarity.findBestMatch(normalizedProduct, avaliableFundsName).bestMatch;
        if (bestMatch.rating >= 0.80) {
            progressState.found++;
            row.bf_cnpj = funds.rows[avaliableFundsNameIndex[bestMatch.target]].f_cnpj;
        } else {
            console.log(`Not found: (${bestMatch.rating.toFixed(2)}): ${normalizedProduct} = ${bestMatch.target}\n`);
        }

        rowsToUpsert.push(row);
        progressState.finished++;
        reportProgress();
    });
    return rowsToUpsert;
};

module.exports = BTGPactualFundWorker;