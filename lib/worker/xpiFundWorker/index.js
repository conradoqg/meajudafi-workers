const Db = require('../../util/db');

const got = require('got');
const allKeys = require('promise-results/allKeys');
const moment = require('moment');

const CONFIG = require('../../util/config');
const TransactionedWorker = require('../transactionedWorker');
const formatters = require('../../util/formatters');

class XPIFundWorker extends TransactionedWorker {
    id = 'XPIFundWorker'
    constructor() {
        super();
    }

    async getData(mainClient, progress) {
        return getFundsData(mainClient, progress);
    }

    async convertData(mainClient, progress, { xpiFunds }) {
        progress.start(xpiFunds.data.length);

        let rowsToUpsert = [];

        for (const item of xpiFunds.data) {
            const row = {
                xf_id: item.id,
                xf_cnpj: item.cnpj,
                xf_name: item.name.replace(/'/g, ''),
                xf_benchmark: item.benchmark.replace(/'/g, ''),
                xf_risk: formatters.parseInt(item.xpRisk),
                xf_morningstar: formatters.parseInt(item.morningstar),
                xf_minimal_initial_investment: formatters.parseFloat(item.minimalInitialInvestment),
                xf_administration_rate: formatters.parseFloat(item.administrationRate) / 100,
                xf_classification_xp: item.classificationXp ? item.classificationXp.replace(/'/g, '') : null,
                xf_investment_quotation_days: formatters.parseInt(item.investmentQuotationDays),
                xf_redemption_quotation_days: formatters.parseInt(item.redemptionQuotationDays),
                xf_redemption_settlement_days: formatters.parseInt(item.redemptionSettlementDays),
                xf_start_date: item.startDate === '' ? null : moment.utc(item.startDate, 'DD/MM/YYYY').toISOString(),
                xf_average_net_equity_12m: formatters.parseFloat(item.averageNetEquity12m),
                xf_net_equity: formatters.parseFloat(item.netEquity),
                xf_max_administration_rate: formatters.parseFloat(item.maxAdministrationRate) / 100,
                xf_trading_account: formatters.parseInt(item.tradingAccount),
                xf_category_code: formatters.parseInt(item.categoryCode),
                xf_risk_genius: formatters.parseInt(item.riskGenius),
                xf_risk_genius_suitability: item.riskGeniusSuitability,
                xf_risk_genius_color: item.riskGeniusColor,
                xf_risk_genius_description: item.riskGeniusDescription ? item.riskGeniusDescription.replace(/'/g, '') : null,
                xf_profitability_month: formatters.parseFloat(item.profitabilityMonth) / 100,
                xf_profitability_12: formatters.parseFloat(item.profitability12) / 100,
                xf_profitability_year: formatters.parseFloat(item.profitabilityYear) / 100,
                xf_funding_blocked: item.fundingBlocked,
                xf_funding_block_justification: item.fundingBlockJustification ? item.fundingBlockJustification.replace(/'/g, '') : null,
                xf_has_lockup: item.hasLockup,
                xf_classification_cvm: item.classificationCvm ? item.classificationCvm.replace(/'/g, '') : null,
                xf_max_performance_rate: formatters.parseFloat(item.maxPerformanceRate) / 100,
                xf_equity: formatters.parseInt(item.equity),
                xf_category_equity: formatters.parseInt(item.categoryEquity),
                xf_block_us_person: item.blockUsPerson,
                xf_is_suggested: item.isSuggested,
                xf_allow_employee: item.allowEmployee,
                xf_allow_general: item.allowGeneral,
                xf_allow_linked: item.allowLinked,
                xf_only_qualified: item.onlyQualified,
                xf_only_professional: item.onlyProfessional,
                xf_document_promotional_material: item.documentPromotionalMaterial,
                xf_document_regulation: item.documentRegulation,
                xf_alkanza: item.alkanza,
                xf_category_name: item.categoryName ? item.categoryName.replace(/'/g, '') : null,
                xf_management_policy: item.managementPolicy ? item.managementPolicy.replace(/'/g, '') : null,
                xf_initials: item.initials,
                xf_redemption_blocked: item.redemptionBlocked,
                xf_objective: item.objective ? item.objective.replace(/'/g, '') : null,
                xf_objective_commercial: item.objectiveCommercial ? item.objectiveCommercial.replace(/'/g, '') : null,
                xf_administration_transfer_rate: formatters.parseFloat(item.administrationTransferRate) / 100,
                xf_return_on_assets: formatters.parseFloat(item.returnOnAssets) / 100
            };

            rowsToUpsert.push(row);

            progress.step();

            await progress.immediate();
        }

        return rowsToUpsert;
    }

    async updateDatabase(mainClient, progress, rowsToUpsert) {
        progress.start();

        // Remove duplicate IDs
        rowsToUpsert = rowsToUpsert.reduce((r, i) => !r.some(j => i.xf_id === j.xf_id) ? [...r, i] : r, []);

        let newQuery = Db.createUpsertQuery({
            table: 'xpi_funds',
            primaryKey: 'xf_id',
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
    const URL = 'https://api.xpi.com.br/investment-funds/yield-portal/v2/investment-funds?family=null';
    return allKeys({
        xpiFunds: got.get(URL, { headers: { 'ocp-apim-subscription-key': '602224c06c434f35b6352ca902c5020a', 'user-agent': 'PostmanRuntime/7.26.8' } }).then(result => JSON.parse(result.body)).catch(ex => { throw new Error(`HTTP get or parse of "${URL}" failed`, ex); })
    });
};

module.exports = XPIFundWorker;