const Db = require('../../util/db');

const got = require('got');
const allKeys = require('promise-results/allKeys');

const TransactionedWorker = require('../transactionedWorker');
const formatters = require('../../util/formatters');
const CONFIG = require('../../util/config');

class BTGPactualFundWorker extends TransactionedWorker {
    id = 'BTGPactualFundWorker'

    constructor() {
        super();
    }

    async getData(mainClient, progress) {
        progress.start();
        return getFundsData();
    }

    async convertData(mainClient, progress, { btgFunds }) {
        progress.start(btgFunds.length);
        let rowsToUpsert = [];

        for await (const item of btgFunds) {
            const row = {
                bf_id: item.id,
                bf_cnpj: item.cnpj,
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

            rowsToUpsert.push(row);
            progress.step();
            await progress.immediate();
        }
        return rowsToUpsert;
    }

    async updateDatabase(mainClient, progress, rowsToUpsert) {
        progress.start();

        rowsToUpsert = rowsToUpsert.reduce((r, i) => !r.some(j => i.bf_id === j.bf_id) ? [...r, i] : r, []);
        
        let newQuery = Db.createUpsertQuery({
            table: 'btgpactual_funds',
            primaryKey: 'bf_id',
            values: rowsToUpsert
        });

        try {
            return await mainClient.query({
                text: newQuery,
                rowMode: 'array'
            });
        } catch (ex) {
            throw new Error(`Query "${CONFIG.QUERY_LOG_SIZE ? newQuery.substr(0, CONFIG.QUERY_LOG_SIZE) : newQuery}" failed`);
        }
    }
}

const getFundsData = async () => {
    const URL = 'https://www.btgpactualdigital.com/services/public/funds/';
    return allKeys({
        btgFunds: got.get(URL).then(result => JSON.parse(result.body)).catch(ex => { throw new Error(`HTTP get or parse of "${URL}" failed`, ex); })
    });
};

module.exports = BTGPactualFundWorker;