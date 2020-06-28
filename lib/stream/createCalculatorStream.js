const uuidv1 = require('uuid/v1');
const stream = require('stream');

const CONFIG = require('../util/config');
const StandardDeviation = require('./../util/standardDeviation');
const string = require('../util/string');

const createCalculatorStream = (startSavingFrom) => {
    let lastChunk = null;
    let chunks = [];
    let chunksByYear = {};
    let chunksByYearAndMonth = {};
    let last21Chunks = [];
    let last63Chunks = [];
    let last126Chunks = [];
    let last252Chunks = [];
    let last504Chunks = [];
    let last756Chunks = [];
    let lastMTDCDIConsistency = {};
    let lastYTDCDIConsistency = {};
    let last21CDIConsistency = [];
    let last63CDIConsistency = [];
    let last126CDIConsistency = [];
    let last252CDIConsistency = [];
    let last504CDIConsistency = [];
    let last756CDIConsistency = [];
    let lastMTDBovespaConsistency = {};
    let lastYTDBovespaConsistency = {};
    let last21BovespaConsistency = [];
    let last63BovespaConsistency = [];
    let last126BovespaConsistency = [];
    let last252BovespaConsistency = [];
    let last504BovespaConsistency = [];
    let last756BovespaConsistency = [];
    let lastMTDIPCAConsistency = {};
    let lastYTDIPCAConsistency = {};
    let last21IPCAConsistency = [];
    let last63IPCAConsistency = [];
    let last126IPCAConsistency = [];
    let last252IPCAConsistency = [];
    let last504IPCAConsistency = [];
    let last756IPCAConsistency = [];
    let lastMTDIGPMConsistency = {};
    let lastYTDIGPMConsistency = {};
    let last21IGPMConsistency = [];
    let last63IGPMConsistency = [];
    let last126IGPMConsistency = [];
    let last252IGPMConsistency = [];
    let last504IGPMConsistency = [];
    let last756IGPMConsistency = [];
    let lastMTDIGPDIConsistency = {};
    let lastYTDIGPDIConsistency = {};
    let last21IGPDIConsistency = [];
    let last63IGPDIConsistency = [];
    let last126IGPDIConsistency = [];
    let last252IGPDIConsistency = [];
    let last504IGPDIConsistency = [];
    let last756IGPDIConsistency = [];
    let lastMTDDolarConsistency = {};
    let lastYTDDolarConsistency = {};
    let last21DolarConsistency = [];
    let last63DolarConsistency = [];
    let last126DolarConsistency = [];
    let last252DolarConsistency = [];
    let last504DolarConsistency = [];
    let last756DolarConsistency = [];
    let lastMTDEuroConsistency = {};
    let lastYTDEuroConsistency = {};
    let last21EuroConsistency = [];
    let last63EuroConsistency = [];
    let last126EuroConsistency = [];
    let last252EuroConsistency = [];
    let last504EuroConsistency = [];
    let last756EuroConsistency = [];
    let cdiConsistencyReachedMTD = {};
    let cdiConsistencyReachedYTD = {};
    let cdiConsistencyReachedLast21 = 0;
    let cdiConsistencyReachedLast63 = 0;
    let cdiConsistencyReachedLast126 = 0;
    let cdiConsistencyReachedLast252 = 0;
    let cdiConsistencyReachedLast504 = 0;
    let cdiConsistencyReachedLast756 = 0;
    let bovespaConsistencyReachedMTD = {};
    let bovespaConsistencyReachedYTD = {};
    let bovespaConsistencyReachedLast21 = 0;
    let bovespaConsistencyReachedLast63 = 0;
    let bovespaConsistencyReachedLast126 = 0;
    let bovespaConsistencyReachedLast252 = 0;
    let bovespaConsistencyReachedLast504 = 0;
    let bovespaConsistencyReachedLast756 = 0;
    let ipcaConsistencyReachedMTD = {};
    let ipcaConsistencyReachedYTD = {};
    let ipcaConsistencyReachedLast21 = 0;
    let ipcaConsistencyReachedLast63 = 0;
    let ipcaConsistencyReachedLast126 = 0;
    let ipcaConsistencyReachedLast252 = 0;
    let ipcaConsistencyReachedLast504 = 0;
    let ipcaConsistencyReachedLast756 = 0;
    let igpmConsistencyReachedMTD = {};
    let igpmConsistencyReachedYTD = {};
    let igpmConsistencyReachedLast21 = 0;
    let igpmConsistencyReachedLast63 = 0;
    let igpmConsistencyReachedLast126 = 0;
    let igpmConsistencyReachedLast252 = 0;
    let igpmConsistencyReachedLast504 = 0;
    let igpmConsistencyReachedLast756 = 0;
    let igpdiConsistencyReachedMTD = {};
    let igpdiConsistencyReachedYTD = {};
    let igpdiConsistencyReachedLast21 = 0;
    let igpdiConsistencyReachedLast63 = 0;
    let igpdiConsistencyReachedLast126 = 0;
    let igpdiConsistencyReachedLast252 = 0;
    let igpdiConsistencyReachedLast504 = 0;
    let igpdiConsistencyReachedLast756 = 0;
    let dolarConsistencyReachedMTD = {};
    let dolarConsistencyReachedYTD = {};
    let dolarConsistencyReachedLast21 = 0;
    let dolarConsistencyReachedLast63 = 0;
    let dolarConsistencyReachedLast126 = 0;
    let dolarConsistencyReachedLast252 = 0;
    let dolarConsistencyReachedLast504 = 0;
    let dolarConsistencyReachedLast756 = 0;
    let euroConsistencyReachedMTD = {};
    let euroConsistencyReachedYTD = {};
    let euroConsistencyReachedLast21 = 0;
    let euroConsistencyReachedLast63 = 0;
    let euroConsistencyReachedLast126 = 0;
    let euroConsistencyReachedLast252 = 0;
    let euroConsistencyReachedLast504 = 0;
    let euroConsistencyReachedLast756 = 0;
    let risk = null;
    let risksByMonth = {};
    let risksByYear = {};
    let last21Risk = null;
    let last63Risk = null;
    let last126Risk = null;
    let last252Risk = null;
    let last504Risk = null;
    let last756Risk = null;
    startSavingFrom = startSavingFrom != null ? `${startSavingFrom.$y}-${string.pad(startSavingFrom.$M + 1, 2)}-${string.pad(startSavingFrom.$D, 2)}` : null;

    const returnFromQuota = (initialQuota, finalQuota) => ((initialQuota == 0) ? 0 : (finalQuota / initialQuota) - 1);
    const returnFromMonthlyTax = (tax) => Math.pow(1 + tax, 1 / 21) - 1;
    const calcSharpeForPeriod = (risk, investment_return, cdi_investment_return, length) => {
        if (risk == 0) return 0;
        const annualizedAccInvestmentReturn = ((investment_return / length) * 252);
        const annualizedAccCDIInvestmentReturn = ((cdi_investment_return / length) * 252);
        return (annualizedAccInvestmentReturn - annualizedAccCDIInvestmentReturn) / risk;
    };
    const calcConsistencyForPeriod = (investment_return, cdi_investment_return, period, consistencyReached, lastConsistency) => {
        let consistencyPoint = 0;
        if (investment_return >= cdi_investment_return) consistencyPoint = 1;
        if (period != 0 && lastConsistency.length >= period) consistencyReached -= lastConsistency.shift();
        consistencyReached += consistencyPoint;
        lastConsistency.push(consistencyPoint);
        return consistencyReached;
    };
    const getConsistencyForPeriod = (consistencyReached, lastConsistency) => ((100 * consistencyReached) / lastConsistency.length) / 100;
    const getMonthHash = (momentDate) => momentDate.$y.toString() + (momentDate.$M + 1).toString();
    const getYearHash = (momentDate) => momentDate.$y.toString();
    const lastItem = (array) => array[array.length - 1];
    const firstItem = (array) => array[0];
    const lastMonthOrFirstDay = (chunk, year, month, lastYear, lastMonth) => {
        if (chunksByYearAndMonth[lastYear] && chunksByYearAndMonth[lastYear][lastMonth]) return lastItem(chunksByYearAndMonth[lastYear][lastMonth]);
        else return firstItem(chunksByYearAndMonth[year][month]);
    };
    const lastYearOrFirstDay = (chunk, year, lastYear) => {
        if (chunksByYearAndMonth[lastYear]) return lastItem(lastItem(Object.values(chunksByYearAndMonth[lastYear])));
        else return firstItem(firstItem(Object.values(chunksByYearAndMonth[year])));
    };

    const processDaily = (stream, chunk, chunkBefore) => {
        let ird_investment_return = 0;
        let ird_cdi_investment_return = 0;
        let ird_bovespa_investment_return = 0;
        let ird_ipca_investment_return = 0;
        let ird_igpm_investment_return = 0;
        let ird_igpdi_investment_return = 0;
        let ird_dolar_investment_return = 0;
        let ird_euro_investment_return = 0;
        let ird_investment_return_mtd = 0;
        let ird_cdi_investment_return_mtd = 0;
        let ird_bovespa_investment_return_mtd = 0;
        let ird_ipca_investment_return_mtd = 0;
        let ird_igpm_investment_return_mtd = 0;
        let ird_igpdi_investment_return_mtd = 0;
        let ird_dolar_investment_return_mtd = 0;
        let ird_euro_investment_return_mtd = 0;
        let ird_investment_return_ytd = 0;
        let ird_cdi_investment_return_ytd = 0;
        let ird_bovespa_investment_return_ytd = 0;
        let ird_ipca_investment_return_ytd = 0;
        let ird_igpm_investment_return_ytd = 0;
        let ird_igpdi_investment_return_ytd = 0;
        let ird_dolar_investment_return_ytd = 0;
        let ird_euro_investment_return_ytd = 0;
        let ird_investment_return_1m = 0;
        let ird_cdi_investment_return_1m = 0;
        let ird_bovespa_investment_return_1m = 0;
        let ird_ipca_investment_return_1m = 0;
        let ird_igpm_investment_return_1m = 0;
        let ird_igpdi_investment_return_1m = 0;
        let ird_dolar_investment_return_1m = 0;
        let ird_euro_investment_return_1m = 0;
        let ird_investment_return_3m = 0;
        let ird_cdi_investment_return_3m = 0;
        let ird_bovespa_investment_return_3m = 0;
        let ird_ipca_investment_return_3m = 0;
        let ird_igpm_investment_return_3m = 0;
        let ird_igpdi_investment_return_3m = 0;
        let ird_dolar_investment_return_3m = 0;
        let ird_euro_investment_return_3m = 0;
        let ird_investment_return_6m = 0;
        let ird_cdi_investment_return_6m = 0;
        let ird_bovespa_investment_return_6m = 0;
        let ird_ipca_investment_return_6m = 0;
        let ird_igpm_investment_return_6m = 0;
        let ird_igpdi_investment_return_6m = 0;
        let ird_dolar_investment_return_6m = 0;
        let ird_euro_investment_return_6m = 0;
        let ird_investment_return_1y = 0;
        let ird_cdi_investment_return_1y = 0;
        let ird_bovespa_investment_return_1y = 0;
        let ird_ipca_investment_return_1y = 0;
        let ird_igpm_investment_return_1y = 0;
        let ird_igpdi_investment_return_1y = 0;
        let ird_dolar_investment_return_1y = 0;
        let ird_euro_investment_return_1y = 0;
        let ird_investment_return_2y = 0;
        let ird_cdi_investment_return_2y = 0;
        let ird_bovespa_investment_return_2y = 0;
        let ird_ipca_investment_return_2y = 0;
        let ird_igpm_investment_return_2y = 0;
        let ird_igpdi_investment_return_2y = 0;
        let ird_dolar_investment_return_2y = 0;
        let ird_euro_investment_return_2y = 0;
        let ird_investment_return_3y = 0;
        let ird_cdi_investment_return_3y = 0;
        let ird_bovespa_investment_return_3y = 0;
        let ird_ipca_investment_return_3y = 0;
        let ird_igpm_investment_return_3y = 0;
        let ird_igpdi_investment_return_3y = 0;
        let ird_dolar_investment_return_3y = 0;
        let ird_euro_investment_return_3y = 0;
        let ird_accumulated_investment_return = 0;
        let ird_cdi_accumulated_investment_return = 0;
        let ird_bovespa_accumulated_investment_return = 0;
        let ird_ipca_accumulated_investment_return = 0;
        let ird_igpm_accumulated_investment_return = 0;
        let ird_igpdi_accumulated_investment_return = 0;
        let ird_dolar_accumulated_investment_return = 0;
        let ird_euro_accumulated_investment_return = 0;
        let ird_risk_mtd = 0;
        let ird_risk_ytd = 0;
        let ird_risk_1m = 0;
        let ird_risk_3m = 0;
        let ird_risk_6m = 0;
        let ird_risk_1y = 0;
        let ird_risk_2y = 0;
        let ird_risk_3y = 0;
        let ird_accumulated_risk = 0;
        let ird_cdi_sharpe_mtd = 0;
        let ird_cdi_sharpe_ytd = 0;
        let ird_cdi_sharpe_1m = 0;
        let ird_cdi_sharpe_3m = 0;
        let ird_cdi_sharpe_6m = 0;
        let ird_cdi_sharpe_1y = 0;
        let ird_cdi_sharpe_2y = 0;
        let ird_cdi_sharpe_3y = 0;
        let ird_cdi_accumulated_sharpe = 0;
        let ird_bovespa_sharpe_mtd = 0;
        let ird_bovespa_sharpe_ytd = 0;
        let ird_bovespa_sharpe_1m = 0;
        let ird_bovespa_sharpe_3m = 0;
        let ird_bovespa_sharpe_6m = 0;
        let ird_bovespa_sharpe_1y = 0;
        let ird_bovespa_sharpe_2y = 0;
        let ird_bovespa_sharpe_3y = 0;
        let ird_bovespa_accumulated_sharpe = 0;
        let ird_ipca_sharpe_mtd = 0;
        let ird_ipca_sharpe_ytd = 0;
        let ird_ipca_sharpe_1m = 0;
        let ird_ipca_sharpe_3m = 0;
        let ird_ipca_sharpe_6m = 0;
        let ird_ipca_sharpe_1y = 0;
        let ird_ipca_sharpe_2y = 0;
        let ird_ipca_sharpe_3y = 0;
        let ird_ipca_accumulated_sharpe = 0;
        let ird_igpm_sharpe_mtd = 0;
        let ird_igpm_sharpe_ytd = 0;
        let ird_igpm_sharpe_1m = 0;
        let ird_igpm_sharpe_3m = 0;
        let ird_igpm_sharpe_6m = 0;
        let ird_igpm_sharpe_1y = 0;
        let ird_igpm_sharpe_2y = 0;
        let ird_igpm_sharpe_3y = 0;
        let ird_igpm_accumulated_sharpe = 0;
        let ird_igpdi_sharpe_mtd = 0;
        let ird_igpdi_sharpe_ytd = 0;
        let ird_igpdi_sharpe_1m = 0;
        let ird_igpdi_sharpe_3m = 0;
        let ird_igpdi_sharpe_6m = 0;
        let ird_igpdi_sharpe_1y = 0;
        let ird_igpdi_sharpe_2y = 0;
        let ird_igpdi_sharpe_3y = 0;
        let ird_igpdi_accumulated_sharpe = 0;
        let ird_dolar_sharpe_mtd = 0;
        let ird_dolar_sharpe_ytd = 0;
        let ird_dolar_sharpe_1m = 0;
        let ird_dolar_sharpe_3m = 0;
        let ird_dolar_sharpe_6m = 0;
        let ird_dolar_sharpe_1y = 0;
        let ird_dolar_sharpe_2y = 0;
        let ird_dolar_sharpe_3y = 0;
        let ird_dolar_accumulated_sharpe = 0;
        let ird_euro_sharpe_mtd = 0;
        let ird_euro_sharpe_ytd = 0;
        let ird_euro_sharpe_1m = 0;
        let ird_euro_sharpe_3m = 0;
        let ird_euro_sharpe_6m = 0;
        let ird_euro_sharpe_1y = 0;
        let ird_euro_sharpe_2y = 0;
        let ird_euro_sharpe_3y = 0;
        let ird_euro_accumulated_sharpe = 0;
        let ird_cdi_consistency_mtd = 0;
        let ird_cdi_consistency_ytd = 0;
        let ird_cdi_consistency_1m = 0;
        let ird_cdi_consistency_3m = 0;
        let ird_cdi_consistency_6m = 0;
        let ird_cdi_consistency_1y = 0;
        let ird_cdi_consistency_2y = 0;
        let ird_cdi_consistency_3y = 0;
        let ird_bovespa_consistency_mtd = 0;
        let ird_bovespa_consistency_ytd = 0;
        let ird_bovespa_consistency_1m = 0;
        let ird_bovespa_consistency_3m = 0;
        let ird_bovespa_consistency_6m = 0;
        let ird_bovespa_consistency_1y = 0;
        let ird_bovespa_consistency_2y = 0;
        let ird_bovespa_consistency_3y = 0;
        let ird_ipca_consistency_mtd = 0;
        let ird_ipca_consistency_ytd = 0;
        let ird_ipca_consistency_1m = 0;
        let ird_ipca_consistency_3m = 0;
        let ird_ipca_consistency_6m = 0;
        let ird_ipca_consistency_1y = 0;
        let ird_ipca_consistency_2y = 0;
        let ird_ipca_consistency_3y = 0;
        let ird_igpm_consistency_mtd = 0;
        let ird_igpm_consistency_ytd = 0;
        let ird_igpm_consistency_1m = 0;
        let ird_igpm_consistency_3m = 0;
        let ird_igpm_consistency_6m = 0;
        let ird_igpm_consistency_1y = 0;
        let ird_igpm_consistency_2y = 0;
        let ird_igpm_consistency_3y = 0;
        let ird_igpdi_consistency_mtd = 0;
        let ird_igpdi_consistency_ytd = 0;
        let ird_igpdi_consistency_1m = 0;
        let ird_igpdi_consistency_3m = 0;
        let ird_igpdi_consistency_6m = 0;
        let ird_igpdi_consistency_1y = 0;
        let ird_igpdi_consistency_2y = 0;
        let ird_igpdi_consistency_3y = 0;
        let ird_dolar_consistency_mtd = 0;
        let ird_dolar_consistency_ytd = 0;
        let ird_dolar_consistency_1m = 0;
        let ird_dolar_consistency_3m = 0;
        let ird_dolar_consistency_6m = 0;
        let ird_dolar_consistency_1y = 0;
        let ird_dolar_consistency_2y = 0;
        let ird_dolar_consistency_3y = 0;
        let ird_euro_consistency_mtd = 0;
        let ird_euro_consistency_ytd = 0;
        let ird_euro_consistency_1m = 0;
        let ird_euro_consistency_3m = 0;
        let ird_euro_consistency_6m = 0;
        let ird_euro_consistency_1y = 0;
        let ird_euro_consistency_2y = 0;
        let ird_euro_consistency_3y = 0;
        let ird_networth = 0;
        let ird_accumulated_networth = 0;
        let ird_networth_mtd = 0;
        let ird_networth_ytd = 0;
        let ird_networth_1m = 0;
        let ird_networth_3m = 0;
        let ird_networth_6m = 0;
        let ird_networth_1y = 0;
        let ird_networth_2y = 0;
        let ird_networth_3y = 0;
        let ird_quotaholders = 0;
        let ird_accumulated_quotaholders = 0;
        let ird_quotaholders_mtd = 0;
        let ird_quotaholders_ytd = 0;
        let ird_quotaholders_1m = 0;
        let ird_quotaholders_3m = 0;
        let ird_quotaholders_6m = 0;
        let ird_quotaholders_1y = 0;
        let ird_quotaholders_2y = 0;
        let ird_quotaholders_3y = 0;

        if (chunkBefore) {
            let change = returnFromQuota(chunkBefore.vl_quota, chunk.vl_quota);
            let cdiChange = returnFromQuota(chunkBefore.cdi_quota, chunk.cdi_quota);
            let bovespaChange = returnFromQuota(chunkBefore.bovespa_valor, chunk.bovespa_valor);
            let ipcaChange = returnFromQuota(chunkBefore.ipca_quota, chunk.ipca_quota);
            let igpmChange = returnFromQuota(chunkBefore.igpm_quota, chunk.igpm_quota);
            let igpdiChange = returnFromQuota(chunkBefore.igpdi_quota, chunk.igpdi_quota);
            let dolarChange = returnFromQuota(chunkBefore.dolar_valor, chunk.dolar_valor);
            let euroChange = returnFromQuota(chunkBefore.euro_valor, chunk.euro_valor);
            let month = getMonthHash(chunk.dt_comptc);
            let year = getYearHash(chunk.dt_comptc);

            // Return
            ird_investment_return = change;
            ird_cdi_investment_return = cdiChange;
            ird_bovespa_investment_return = bovespaChange;
            ird_ipca_investment_return = ipcaChange;
            ird_igpm_investment_return = igpmChange;
            ird_igpdi_investment_return = igpdiChange;
            ird_dolar_investment_return = dolarChange;
            ird_euro_investment_return = euroChange;

            // Return MTD
            ird_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].vl_quota, chunk.vl_quota);
            ird_cdi_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].bovespa_valor, chunk.bovespa_valor);
            ird_ipca_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].ipca_quota, chunk.ipca_quota);
            ird_igpm_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].igpm_quota, chunk.igpm_quota);
            ird_igpdi_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].igpdi_quota, chunk.igpdi_quota);
            ird_dolar_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].dolar_valor, chunk.dolar_valor);
            ird_euro_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].euro_valor, chunk.euro_valor);

            // Return YTD
            ird_investment_return_ytd = returnFromQuota(chunksByYear[year][0].vl_quota, chunk.vl_quota);
            ird_cdi_investment_return_ytd = returnFromQuota(chunksByYear[year][0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_investment_return_ytd = returnFromQuota(chunksByYear[year][0].bovespa_valor, chunk.bovespa_valor);
            ird_ipca_investment_return_ytd = returnFromQuota(chunksByYear[year][0].ipca_quota, chunk.ipca_quota);
            ird_igpm_investment_return_ytd = returnFromQuota(chunksByYear[year][0].igpm_quota, chunk.igpm_quota);
            ird_igpdi_investment_return_ytd = returnFromQuota(chunksByYear[year][0].igpdi_quota, chunk.igpdi_quota);
            ird_dolar_investment_return_ytd = returnFromQuota(chunksByYear[year][0].dolar_valor, chunk.dolar_valor);
            ird_euro_investment_return_ytd = returnFromQuota(chunksByYear[year][0].euro_valor, chunk.euro_valor);

            // Return 1M
            ird_investment_return_1m = returnFromQuota(last21Chunks[0].vl_quota, chunk.vl_quota);
            ird_cdi_investment_return_1m = returnFromQuota(last21Chunks[0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_investment_return_1m = returnFromQuota(last21Chunks[0].bovespa_valor, chunk.bovespa_valor);
            ird_ipca_investment_return_1m = returnFromQuota(last21Chunks[0].ipca_quota, chunk.ipca_quota);
            ird_igpm_investment_return_1m = returnFromQuota(last21Chunks[0].igpm_quota, chunk.igpm_quota);
            ird_igpdi_investment_return_1m = returnFromQuota(last21Chunks[0].igpdi_quota, chunk.igpdi_quota);
            ird_dolar_investment_return_1m = returnFromQuota(last21Chunks[0].dolar_valor, chunk.dolar_valor);
            ird_euro_investment_return_1m = returnFromQuota(last21Chunks[0].euro_valor, chunk.euro_valor);

            // Return 3M
            ird_investment_return_3m = returnFromQuota(last63Chunks[0].vl_quota, chunk.vl_quota);
            ird_cdi_investment_return_3m = returnFromQuota(last63Chunks[0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_investment_return_3m = returnFromQuota(last63Chunks[0].bovespa_valor, chunk.bovespa_valor);
            ird_ipca_investment_return_3m = returnFromQuota(last63Chunks[0].ipca_quota, chunk.ipca_quota);
            ird_igpm_investment_return_3m = returnFromQuota(last63Chunks[0].igpm_quota, chunk.igpm_quota);
            ird_igpdi_investment_return_3m = returnFromQuota(last63Chunks[0].igpdi_quota, chunk.igpdi_quota);
            ird_dolar_investment_return_3m = returnFromQuota(last63Chunks[0].dolar_valor, chunk.dolar_valor);
            ird_euro_investment_return_3m = returnFromQuota(last63Chunks[0].euro_valor, chunk.euro_valor);

            // Return 6M
            ird_investment_return_6m = returnFromQuota(last126Chunks[0].vl_quota, chunk.vl_quota);
            ird_cdi_investment_return_6m = returnFromQuota(last126Chunks[0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_investment_return_6m = returnFromQuota(last126Chunks[0].bovespa_valor, chunk.bovespa_valor);
            ird_ipca_investment_return_6m = returnFromQuota(last126Chunks[0].ipca_quota, chunk.ipca_quota);
            ird_igpm_investment_return_6m = returnFromQuota(last126Chunks[0].igpm_quota, chunk.igpm_quota);
            ird_igpdi_investment_return_6m = returnFromQuota(last126Chunks[0].igpdi_quota, chunk.igpdi_quota);
            ird_dolar_investment_return_6m = returnFromQuota(last126Chunks[0].dolar_valor, chunk.dolar_valor);
            ird_euro_investment_return_6m = returnFromQuota(last126Chunks[0].euro_valor, chunk.euro_valor);

            // Return 1Y
            ird_investment_return_1y = returnFromQuota(last252Chunks[0].vl_quota, chunk.vl_quota);
            ird_cdi_investment_return_1y = returnFromQuota(last252Chunks[0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_investment_return_1y = returnFromQuota(last252Chunks[0].bovespa_valor, chunk.bovespa_valor);
            ird_ipca_investment_return_1y = returnFromQuota(last252Chunks[0].ipca_quota, chunk.ipca_quota);
            ird_igpm_investment_return_1y = returnFromQuota(last252Chunks[0].igpm_quota, chunk.igpm_quota);
            ird_igpdi_investment_return_1y = returnFromQuota(last252Chunks[0].igpdi_quota, chunk.igpdi_quota);
            ird_dolar_investment_return_1y = returnFromQuota(last252Chunks[0].dolar_valor, chunk.dolar_valor);
            ird_euro_investment_return_1y = returnFromQuota(last252Chunks[0].euro_valor, chunk.euro_valor);

            // Return 2Y        
            ird_investment_return_2y = returnFromQuota(last504Chunks[0].vl_quota, chunk.vl_quota);
            ird_cdi_investment_return_2y = returnFromQuota(last504Chunks[0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_investment_return_2y = returnFromQuota(last504Chunks[0].bovespa_valor, chunk.bovespa_valor);
            ird_ipca_investment_return_2y = returnFromQuota(last504Chunks[0].ipca_quota, chunk.ipca_quota);
            ird_igpm_investment_return_2y = returnFromQuota(last504Chunks[0].igpm_quota, chunk.igpm_quota);
            ird_igpdi_investment_return_2y = returnFromQuota(last504Chunks[0].igpdi_quota, chunk.igpdi_quota);
            ird_dolar_investment_return_2y = returnFromQuota(last504Chunks[0].dolar_valor, chunk.dolar_valor);
            ird_euro_investment_return_2y = returnFromQuota(last504Chunks[0].euro_valor, chunk.euro_valor);

            // Return 3Y        
            ird_investment_return_3y = returnFromQuota(last756Chunks[0].vl_quota, chunk.vl_quota);
            ird_cdi_investment_return_3y = returnFromQuota(last756Chunks[0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_investment_return_3y = returnFromQuota(last756Chunks[0].bovespa_valor, chunk.bovespa_valor);
            ird_ipca_investment_return_3y = returnFromQuota(last756Chunks[0].ipca_quota, chunk.ipca_quota);
            ird_igpm_investment_return_3y = returnFromQuota(last756Chunks[0].igpm_quota, chunk.igpm_quota);
            ird_igpdi_investment_return_3y = returnFromQuota(last756Chunks[0].igpdi_quota, chunk.igpdi_quota);
            ird_dolar_investment_return_3y = returnFromQuota(last756Chunks[0].dolar_valor, chunk.dolar_valor);
            ird_euro_investment_return_3y = returnFromQuota(last756Chunks[0].euro_valor, chunk.euro_valor);

            // Accumulated Return        
            ird_accumulated_investment_return = returnFromQuota(chunks[0].vl_quota, chunk.vl_quota);
            ird_cdi_accumulated_investment_return = returnFromQuota(chunks[0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_accumulated_investment_return = returnFromQuota(chunks[0].bovespa_valor, chunk.bovespa_valor);
            ird_ipca_accumulated_investment_return = returnFromQuota(chunks[0].ipca_quota, chunk.ipca_quota);
            ird_igpm_accumulated_investment_return = returnFromQuota(chunks[0].igpm_quota, chunk.igpm_quota);
            ird_igpdi_accumulated_investment_return = returnFromQuota(chunks[0].igpdi_quota, chunk.igpdi_quota);
            ird_dolar_accumulated_investment_return = returnFromQuota(chunks[0].dolar_valor, chunk.dolar_valor);
            ird_euro_accumulated_investment_return = returnFromQuota(chunks[0].euro_valor, chunk.euro_valor);

            // Risk MTD
            if (!risksByMonth[month]) risksByMonth[month] = new StandardDeviation(change);
            else risksByMonth[month].addMeasurement(change);
            ird_risk_mtd = risksByMonth[month].get() * Math.sqrt(252);

            // Risk YTD
            if (!risksByYear[year]) risksByYear[year] = new StandardDeviation(change);
            else risksByYear[year].addMeasurement(change);
            ird_risk_ytd = risksByYear[year].get() * Math.sqrt(252);

            // Risk 1M
            if (last21Risk == null) last21Risk = new StandardDeviation(change);
            else last21Risk.addMeasurement(change);
            ird_risk_1m = last21Risk.get() * Math.sqrt(252);

            // Risk 3M
            if (last63Risk == null) last63Risk = new StandardDeviation(change);
            else last63Risk.addMeasurement(change);
            ird_risk_3m = last63Risk.get() * Math.sqrt(252);

            // Risk 6M
            if (last126Risk == null) last126Risk = new StandardDeviation(change);
            else last126Risk.addMeasurement(change);
            ird_risk_6m = last126Risk.get() * Math.sqrt(252);

            // Risk 1Y
            if (last252Risk == null) last252Risk = new StandardDeviation(change);
            else last252Risk.addMeasurement(change);
            ird_risk_1y = last252Risk.get() * Math.sqrt(252);

            // Risk 2Y
            if (last504Risk == null) last504Risk = new StandardDeviation(change);
            else last504Risk.addMeasurement(change);
            ird_risk_2y = last504Risk.get() * Math.sqrt(252);

            // Risk 3Y
            if (last756Risk == null) last756Risk = new StandardDeviation(change);
            else last756Risk.addMeasurement(change);
            ird_risk_3y = last756Risk.get() * Math.sqrt(252);

            // Accumulated Risk
            if (risk == null) risk = new StandardDeviation(change);
            else risk.addMeasurement(change);
            ird_accumulated_risk = risk.get() * Math.sqrt(252);

            // Sharpe MTD
            ird_cdi_sharpe_mtd = calcSharpeForPeriod(ird_risk_mtd, ird_investment_return_mtd, ird_cdi_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            ird_bovespa_sharpe_mtd = calcSharpeForPeriod(ird_risk_mtd, ird_investment_return_mtd, ird_bovespa_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            ird_ipca_sharpe_mtd = calcSharpeForPeriod(ird_risk_mtd, ird_investment_return_mtd, ird_ipca_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            ird_igpm_sharpe_mtd = calcSharpeForPeriod(ird_risk_mtd, ird_investment_return_mtd, ird_igpm_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            ird_igpdi_sharpe_mtd = calcSharpeForPeriod(ird_risk_mtd, ird_investment_return_mtd, ird_igpdi_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            ird_dolar_sharpe_mtd = calcSharpeForPeriod(ird_risk_mtd, ird_investment_return_mtd, ird_dolar_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            ird_euro_sharpe_mtd = calcSharpeForPeriod(ird_risk_mtd, ird_investment_return_mtd, ird_euro_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);

            // Sharpe YTD
            ird_cdi_sharpe_ytd = calcSharpeForPeriod(ird_risk_ytd, ird_investment_return_ytd, ird_cdi_investment_return_ytd, chunksByYear[year].length - 1);
            ird_bovespa_sharpe_ytd = calcSharpeForPeriod(ird_risk_ytd, ird_investment_return_ytd, ird_bovespa_investment_return_ytd, chunksByYear[year].length - 1);
            ird_ipca_sharpe_ytd = calcSharpeForPeriod(ird_risk_ytd, ird_investment_return_ytd, ird_ipca_investment_return_ytd, chunksByYear[year].length - 1);
            ird_igpm_sharpe_ytd = calcSharpeForPeriod(ird_risk_ytd, ird_investment_return_ytd, ird_igpm_investment_return_ytd, chunksByYear[year].length - 1);
            ird_igpdi_sharpe_ytd = calcSharpeForPeriod(ird_risk_ytd, ird_investment_return_ytd, ird_igpdi_investment_return_ytd, chunksByYear[year].length - 1);
            ird_dolar_sharpe_ytd = calcSharpeForPeriod(ird_risk_ytd, ird_investment_return_ytd, ird_dolar_investment_return_ytd, chunksByYear[year].length - 1);
            ird_euro_sharpe_ytd = calcSharpeForPeriod(ird_risk_ytd, ird_investment_return_ytd, ird_euro_investment_return_ytd, chunksByYear[year].length - 1);

            // Sharpe 1M
            ird_cdi_sharpe_1m = calcSharpeForPeriod(ird_risk_1m, ird_investment_return_1m, ird_cdi_investment_return_1m, last21Chunks.length - 1);
            ird_bovespa_sharpe_1m = calcSharpeForPeriod(ird_risk_1m, ird_investment_return_1m, ird_bovespa_investment_return_1m, last21Chunks.length - 1);
            ird_ipca_sharpe_1m = calcSharpeForPeriod(ird_risk_1m, ird_investment_return_1m, ird_ipca_investment_return_1m, last21Chunks.length - 1);
            ird_igpm_sharpe_1m = calcSharpeForPeriod(ird_risk_1m, ird_investment_return_1m, ird_igpm_investment_return_1m, last21Chunks.length - 1);
            ird_igpdi_sharpe_1m = calcSharpeForPeriod(ird_risk_1m, ird_investment_return_1m, ird_igpdi_investment_return_1m, last21Chunks.length - 1);
            ird_dolar_sharpe_1m = calcSharpeForPeriod(ird_risk_1m, ird_investment_return_1m, ird_dolar_investment_return_1m, last21Chunks.length - 1);
            ird_euro_sharpe_1m = calcSharpeForPeriod(ird_risk_1m, ird_investment_return_1m, ird_euro_investment_return_1m, last21Chunks.length - 1);

            // Sharpe 3M
            ird_cdi_sharpe_3m = calcSharpeForPeriod(ird_risk_3m, ird_investment_return_3m, ird_cdi_investment_return_3m, last63Chunks.length - 1);
            ird_bovespa_sharpe_3m = calcSharpeForPeriod(ird_risk_3m, ird_investment_return_3m, ird_bovespa_investment_return_3m, last63Chunks.length - 1);
            ird_ipca_sharpe_3m = calcSharpeForPeriod(ird_risk_3m, ird_investment_return_3m, ird_ipca_investment_return_3m, last63Chunks.length - 1);
            ird_igpm_sharpe_3m = calcSharpeForPeriod(ird_risk_3m, ird_investment_return_3m, ird_igpm_investment_return_3m, last63Chunks.length - 1);
            ird_igpdi_sharpe_3m = calcSharpeForPeriod(ird_risk_3m, ird_investment_return_3m, ird_igpdi_investment_return_3m, last63Chunks.length - 1);
            ird_dolar_sharpe_3m = calcSharpeForPeriod(ird_risk_3m, ird_investment_return_3m, ird_dolar_investment_return_3m, last63Chunks.length - 1);
            ird_euro_sharpe_3m = calcSharpeForPeriod(ird_risk_3m, ird_investment_return_3m, ird_euro_investment_return_3m, last63Chunks.length - 1);

            // Sharpe 6M
            ird_cdi_sharpe_6m = calcSharpeForPeriod(ird_risk_6m, ird_investment_return_6m, ird_cdi_investment_return_6m, last126Chunks.length - 1);
            ird_bovespa_sharpe_6m = calcSharpeForPeriod(ird_risk_6m, ird_investment_return_6m, ird_bovespa_investment_return_6m, last126Chunks.length - 1);
            ird_ipca_sharpe_6m = calcSharpeForPeriod(ird_risk_6m, ird_investment_return_6m, ird_ipca_investment_return_6m, last126Chunks.length - 1);
            ird_igpm_sharpe_6m = calcSharpeForPeriod(ird_risk_6m, ird_investment_return_6m, ird_igpm_investment_return_6m, last126Chunks.length - 1);
            ird_igpdi_sharpe_6m = calcSharpeForPeriod(ird_risk_6m, ird_investment_return_6m, ird_igpdi_investment_return_6m, last126Chunks.length - 1);
            ird_dolar_sharpe_6m = calcSharpeForPeriod(ird_risk_6m, ird_investment_return_6m, ird_dolar_investment_return_6m, last126Chunks.length - 1);
            ird_euro_sharpe_6m = calcSharpeForPeriod(ird_risk_6m, ird_investment_return_6m, ird_euro_investment_return_6m, last126Chunks.length - 1);

            // Sharpe 1Y     
            ird_cdi_sharpe_1y = calcSharpeForPeriod(ird_risk_1y, ird_investment_return_1y, ird_cdi_investment_return_1y, last252Chunks.length - 1);
            ird_bovespa_sharpe_1y = calcSharpeForPeriod(ird_risk_1y, ird_investment_return_1y, ird_bovespa_investment_return_1y, last252Chunks.length - 1);
            ird_ipca_sharpe_1y = calcSharpeForPeriod(ird_risk_1y, ird_investment_return_1y, ird_ipca_investment_return_1y, last252Chunks.length - 1);
            ird_igpm_sharpe_1y = calcSharpeForPeriod(ird_risk_1y, ird_investment_return_1y, ird_igpm_investment_return_1y, last252Chunks.length - 1);
            ird_igpdi_sharpe_1y = calcSharpeForPeriod(ird_risk_1y, ird_investment_return_1y, ird_igpdi_investment_return_1y, last252Chunks.length - 1);
            ird_dolar_sharpe_1y = calcSharpeForPeriod(ird_risk_1y, ird_investment_return_1y, ird_dolar_investment_return_1y, last252Chunks.length - 1);
            ird_euro_sharpe_1y = calcSharpeForPeriod(ird_risk_1y, ird_investment_return_1y, ird_euro_investment_return_1y, last252Chunks.length - 1);

            // Sharpe 2Y                
            ird_cdi_sharpe_2y = calcSharpeForPeriod(ird_risk_2y, ird_investment_return_2y, ird_cdi_investment_return_2y, last504Chunks.length - 1);
            ird_bovespa_sharpe_2y = calcSharpeForPeriod(ird_risk_2y, ird_investment_return_2y, ird_bovespa_investment_return_2y, last504Chunks.length - 1);
            ird_ipca_sharpe_2y = calcSharpeForPeriod(ird_risk_2y, ird_investment_return_2y, ird_ipca_investment_return_2y, last504Chunks.length - 1);
            ird_igpm_sharpe_2y = calcSharpeForPeriod(ird_risk_2y, ird_investment_return_2y, ird_igpm_investment_return_2y, last504Chunks.length - 1);
            ird_igpdi_sharpe_2y = calcSharpeForPeriod(ird_risk_2y, ird_investment_return_2y, ird_igpdi_investment_return_2y, last504Chunks.length - 1);
            ird_dolar_sharpe_2y = calcSharpeForPeriod(ird_risk_2y, ird_investment_return_2y, ird_dolar_investment_return_2y, last504Chunks.length - 1);
            ird_euro_sharpe_2y = calcSharpeForPeriod(ird_risk_2y, ird_investment_return_2y, ird_euro_investment_return_2y, last504Chunks.length - 1);

            // Sharpe 3Y        
            ird_cdi_sharpe_3y = calcSharpeForPeriod(ird_risk_3y, ird_investment_return_3y, ird_cdi_investment_return_3y, last756Chunks.length - 1);
            ird_bovespa_sharpe_3y = calcSharpeForPeriod(ird_risk_3y, ird_investment_return_3y, ird_bovespa_investment_return_3y, last756Chunks.length - 1);
            ird_ipca_sharpe_3y = calcSharpeForPeriod(ird_risk_3y, ird_investment_return_3y, ird_ipca_investment_return_3y, last756Chunks.length - 1);
            ird_igpm_sharpe_3y = calcSharpeForPeriod(ird_risk_3y, ird_investment_return_3y, ird_igpm_investment_return_3y, last756Chunks.length - 1);
            ird_igpdi_sharpe_3y = calcSharpeForPeriod(ird_risk_3y, ird_investment_return_3y, ird_igpdi_investment_return_3y, last756Chunks.length - 1);
            ird_dolar_sharpe_3y = calcSharpeForPeriod(ird_risk_3y, ird_investment_return_3y, ird_dolar_investment_return_3y, last756Chunks.length - 1);
            ird_euro_sharpe_3y = calcSharpeForPeriod(ird_risk_3y, ird_investment_return_3y, ird_euro_investment_return_3y, last756Chunks.length - 1);

            // Accumulated Sharpe        
            ird_cdi_accumulated_sharpe = calcSharpeForPeriod(ird_accumulated_risk, ird_accumulated_investment_return, ird_cdi_accumulated_investment_return, chunks.length - 1);
            ird_bovespa_accumulated_sharpe = calcSharpeForPeriod(ird_accumulated_risk, ird_accumulated_investment_return, ird_bovespa_accumulated_investment_return, chunks.length - 1);
            ird_ipca_accumulated_sharpe = calcSharpeForPeriod(ird_accumulated_risk, ird_accumulated_investment_return, ird_ipca_accumulated_investment_return, chunks.length - 1);
            ird_igpm_accumulated_sharpe = calcSharpeForPeriod(ird_accumulated_risk, ird_accumulated_investment_return, ird_igpm_accumulated_investment_return, chunks.length - 1);
            ird_igpdi_accumulated_sharpe = calcSharpeForPeriod(ird_accumulated_risk, ird_accumulated_investment_return, ird_igpdi_accumulated_investment_return, chunks.length - 1);
            ird_dolar_accumulated_sharpe = calcSharpeForPeriod(ird_accumulated_risk, ird_accumulated_investment_return, ird_dolar_accumulated_investment_return, chunks.length - 1);
            ird_euro_accumulated_sharpe = calcSharpeForPeriod(ird_accumulated_risk, ird_accumulated_investment_return, ird_euro_accumulated_investment_return, chunks.length - 1);

            // Consistency MTD        
            cdiConsistencyReachedMTD[month] = calcConsistencyForPeriod(ird_investment_return_mtd, ird_cdi_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1, cdiConsistencyReachedMTD[month], lastMTDCDIConsistency[month]);
            ird_cdi_consistency_mtd = getConsistencyForPeriod(cdiConsistencyReachedMTD[month], lastMTDCDIConsistency[month]);
            bovespaConsistencyReachedMTD[month] = calcConsistencyForPeriod(ird_investment_return_mtd, ird_bovespa_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1, bovespaConsistencyReachedMTD[month], lastMTDBovespaConsistency[month]);
            ird_bovespa_consistency_mtd = getConsistencyForPeriod(bovespaConsistencyReachedMTD[month], lastMTDBovespaConsistency[month]);
            ipcaConsistencyReachedMTD[month] = calcConsistencyForPeriod(ird_investment_return_mtd, ird_ipca_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1, ipcaConsistencyReachedMTD[month], lastMTDIPCAConsistency[month]);
            ird_ipca_consistency_mtd = getConsistencyForPeriod(ipcaConsistencyReachedMTD[month], lastMTDIPCAConsistency[month]);
            igpmConsistencyReachedMTD[month] = calcConsistencyForPeriod(ird_investment_return_mtd, ird_igpm_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1, igpmConsistencyReachedMTD[month], lastMTDIGPMConsistency[month]);
            ird_igpm_consistency_mtd = getConsistencyForPeriod(igpmConsistencyReachedMTD[month], lastMTDIGPMConsistency[month]);
            igpdiConsistencyReachedMTD[month] = calcConsistencyForPeriod(ird_investment_return_mtd, ird_igpdi_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1, igpdiConsistencyReachedMTD[month], lastMTDIGPDIConsistency[month]);
            ird_igpdi_consistency_mtd = getConsistencyForPeriod(igpdiConsistencyReachedMTD[month], lastMTDIGPDIConsistency[month]);
            dolarConsistencyReachedMTD[month] = calcConsistencyForPeriod(ird_investment_return_mtd, ird_dolar_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1, dolarConsistencyReachedMTD[month], lastMTDDolarConsistency[month]);
            ird_dolar_consistency_mtd = getConsistencyForPeriod(dolarConsistencyReachedMTD[month], lastMTDDolarConsistency[month]);
            euroConsistencyReachedMTD[month] = calcConsistencyForPeriod(ird_investment_return_mtd, ird_euro_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1, euroConsistencyReachedMTD[month], lastMTDEuroConsistency[month]);
            ird_euro_consistency_mtd = getConsistencyForPeriod(euroConsistencyReachedMTD[month], lastMTDEuroConsistency[month]);

            // Consistency YTD        
            cdiConsistencyReachedYTD[year] = calcConsistencyForPeriod(ird_investment_return_ytd, ird_cdi_investment_return_ytd, chunksByYear[year].length - 1, cdiConsistencyReachedYTD[year], lastYTDCDIConsistency[year]);
            ird_cdi_consistency_ytd = getConsistencyForPeriod(cdiConsistencyReachedYTD[year], lastYTDCDIConsistency[year]);
            bovespaConsistencyReachedYTD[year] = calcConsistencyForPeriod(ird_investment_return_ytd, ird_bovespa_investment_return_ytd, chunksByYear[year].length - 1, bovespaConsistencyReachedYTD[year], lastYTDBovespaConsistency[year]);
            ird_bovespa_consistency_ytd = getConsistencyForPeriod(bovespaConsistencyReachedYTD[year], lastYTDBovespaConsistency[year]);
            ipcaConsistencyReachedYTD[year] = calcConsistencyForPeriod(ird_investment_return_ytd, ird_ipca_investment_return_ytd, chunksByYear[year].length - 1, ipcaConsistencyReachedYTD[year], lastYTDIPCAConsistency[year]);
            ird_ipca_consistency_ytd = getConsistencyForPeriod(ipcaConsistencyReachedYTD[year], lastYTDIPCAConsistency[year]);
            igpmConsistencyReachedYTD[year] = calcConsistencyForPeriod(ird_investment_return_ytd, ird_igpm_investment_return_ytd, chunksByYear[year].length - 1, igpmConsistencyReachedYTD[year], lastYTDIGPMConsistency[year]);
            ird_igpm_consistency_ytd = getConsistencyForPeriod(igpmConsistencyReachedYTD[year], lastYTDIGPMConsistency[year]);
            igpdiConsistencyReachedYTD[year] = calcConsistencyForPeriod(ird_investment_return_ytd, ird_igpdi_investment_return_ytd, chunksByYear[year].length - 1, igpdiConsistencyReachedYTD[year], lastYTDIGPDIConsistency[year]);
            ird_igpdi_consistency_ytd = getConsistencyForPeriod(igpdiConsistencyReachedYTD[year], lastYTDIGPDIConsistency[year]);
            dolarConsistencyReachedYTD[year] = calcConsistencyForPeriod(ird_investment_return_ytd, ird_dolar_investment_return_ytd, chunksByYear[year].length - 1, dolarConsistencyReachedYTD[year], lastYTDDolarConsistency[year]);
            ird_dolar_consistency_ytd = getConsistencyForPeriod(dolarConsistencyReachedYTD[year], lastYTDDolarConsistency[year]);
            euroConsistencyReachedYTD[year] = calcConsistencyForPeriod(ird_investment_return_ytd, ird_euro_investment_return_ytd, chunksByYear[year].length - 1, euroConsistencyReachedYTD[year], lastYTDEuroConsistency[year]);
            ird_euro_consistency_ytd = getConsistencyForPeriod(euroConsistencyReachedYTD[year], lastYTDEuroConsistency[year]);

            // Consistency 1M        
            cdiConsistencyReachedLast21 = calcConsistencyForPeriod(ird_investment_return_1m, ird_cdi_investment_return_1m, 21, cdiConsistencyReachedLast21, last21CDIConsistency);
            ird_cdi_consistency_1m = getConsistencyForPeriod(cdiConsistencyReachedLast21, last21CDIConsistency);
            bovespaConsistencyReachedLast21 = calcConsistencyForPeriod(ird_investment_return_1m, ird_bovespa_investment_return_1m, 21, bovespaConsistencyReachedLast21, last21BovespaConsistency);
            ird_bovespa_consistency_1m = getConsistencyForPeriod(bovespaConsistencyReachedLast21, last21BovespaConsistency);
            ipcaConsistencyReachedLast21 = calcConsistencyForPeriod(ird_investment_return_1m, ird_ipca_investment_return_1m, 21, ipcaConsistencyReachedLast21, last21IPCAConsistency);
            ird_ipca_consistency_1m = getConsistencyForPeriod(ipcaConsistencyReachedLast21, last21IPCAConsistency);
            igpmConsistencyReachedLast21 = calcConsistencyForPeriod(ird_investment_return_1m, ird_igpm_investment_return_1m, 21, igpmConsistencyReachedLast21, last21IGPMConsistency);
            ird_igpm_consistency_1m = getConsistencyForPeriod(igpmConsistencyReachedLast21, last21IGPMConsistency);
            igpdiConsistencyReachedLast21 = calcConsistencyForPeriod(ird_investment_return_1m, ird_igpdi_investment_return_1m, 21, igpdiConsistencyReachedLast21, last21IGPDIConsistency);
            ird_igpdi_consistency_1m = getConsistencyForPeriod(igpdiConsistencyReachedLast21, last21IGPDIConsistency);
            dolarConsistencyReachedLast21 = calcConsistencyForPeriod(ird_investment_return_1m, ird_dolar_investment_return_1m, 21, dolarConsistencyReachedLast21, last21DolarConsistency);
            ird_dolar_consistency_1m = getConsistencyForPeriod(dolarConsistencyReachedLast21, last21DolarConsistency);
            euroConsistencyReachedLast21 = calcConsistencyForPeriod(ird_investment_return_1m, ird_euro_investment_return_1m, 21, euroConsistencyReachedLast21, last21EuroConsistency);
            ird_euro_consistency_1m = getConsistencyForPeriod(euroConsistencyReachedLast21, last21EuroConsistency);

            // Consistency 3M
            cdiConsistencyReachedLast63 = calcConsistencyForPeriod(ird_investment_return_3m, ird_cdi_investment_return_3m, 63, cdiConsistencyReachedLast63, last63CDIConsistency);
            ird_cdi_consistency_3m = getConsistencyForPeriod(cdiConsistencyReachedLast63, last63CDIConsistency);
            bovespaConsistencyReachedLast63 = calcConsistencyForPeriod(ird_investment_return_3m, ird_bovespa_investment_return_3m, 63, bovespaConsistencyReachedLast63, last63BovespaConsistency);
            ird_bovespa_consistency_3m = getConsistencyForPeriod(bovespaConsistencyReachedLast63, last63BovespaConsistency);
            ipcaConsistencyReachedLast63 = calcConsistencyForPeriod(ird_investment_return_3m, ird_ipca_investment_return_3m, 63, ipcaConsistencyReachedLast63, last63IPCAConsistency);
            ird_ipca_consistency_3m = getConsistencyForPeriod(ipcaConsistencyReachedLast63, last63IPCAConsistency);
            igpmConsistencyReachedLast63 = calcConsistencyForPeriod(ird_investment_return_3m, ird_igpm_investment_return_3m, 63, igpmConsistencyReachedLast63, last63IGPMConsistency);
            ird_igpm_consistency_3m = getConsistencyForPeriod(igpmConsistencyReachedLast63, last63IGPMConsistency);
            igpdiConsistencyReachedLast63 = calcConsistencyForPeriod(ird_investment_return_3m, ird_igpdi_investment_return_3m, 63, igpdiConsistencyReachedLast63, last63IGPDIConsistency);
            ird_igpdi_consistency_3m = getConsistencyForPeriod(igpdiConsistencyReachedLast63, last63IGPDIConsistency);
            dolarConsistencyReachedLast63 = calcConsistencyForPeriod(ird_investment_return_3m, ird_dolar_investment_return_3m, 63, dolarConsistencyReachedLast63, last63DolarConsistency);
            ird_dolar_consistency_3m = getConsistencyForPeriod(dolarConsistencyReachedLast63, last63DolarConsistency);
            euroConsistencyReachedLast63 = calcConsistencyForPeriod(ird_investment_return_3m, ird_euro_investment_return_3m, 63, euroConsistencyReachedLast63, last63EuroConsistency);
            ird_euro_consistency_3m = getConsistencyForPeriod(euroConsistencyReachedLast63, last63EuroConsistency);

            // Consistency 6M
            cdiConsistencyReachedLast126 = calcConsistencyForPeriod(ird_investment_return_6m, ird_cdi_investment_return_6m, 126, cdiConsistencyReachedLast126, last126CDIConsistency);
            ird_cdi_consistency_6m = getConsistencyForPeriod(cdiConsistencyReachedLast126, last126CDIConsistency);
            bovespaConsistencyReachedLast126 = calcConsistencyForPeriod(ird_investment_return_6m, ird_bovespa_investment_return_6m, 126, bovespaConsistencyReachedLast126, last126BovespaConsistency);
            ird_bovespa_consistency_6m = getConsistencyForPeriod(bovespaConsistencyReachedLast126, last126BovespaConsistency);
            ipcaConsistencyReachedLast126 = calcConsistencyForPeriod(ird_investment_return_6m, ird_ipca_investment_return_6m, 126, ipcaConsistencyReachedLast126, last126IPCAConsistency);
            ird_ipca_consistency_6m = getConsistencyForPeriod(ipcaConsistencyReachedLast126, last126IPCAConsistency);
            igpmConsistencyReachedLast126 = calcConsistencyForPeriod(ird_investment_return_6m, ird_igpm_investment_return_6m, 126, igpmConsistencyReachedLast126, last126IGPMConsistency);
            ird_igpm_consistency_6m = getConsistencyForPeriod(igpmConsistencyReachedLast126, last126IGPMConsistency);
            igpdiConsistencyReachedLast126 = calcConsistencyForPeriod(ird_investment_return_6m, ird_igpdi_investment_return_6m, 126, igpdiConsistencyReachedLast126, last126IGPDIConsistency);
            ird_igpdi_consistency_6m = getConsistencyForPeriod(igpdiConsistencyReachedLast126, last126IGPDIConsistency);
            dolarConsistencyReachedLast126 = calcConsistencyForPeriod(ird_investment_return_6m, ird_dolar_investment_return_6m, 126, dolarConsistencyReachedLast126, last126DolarConsistency);
            ird_dolar_consistency_6m = getConsistencyForPeriod(dolarConsistencyReachedLast126, last126DolarConsistency);
            euroConsistencyReachedLast126 = calcConsistencyForPeriod(ird_investment_return_6m, ird_euro_investment_return_6m, 126, euroConsistencyReachedLast126, last126EuroConsistency);
            ird_euro_consistency_6m = getConsistencyForPeriod(euroConsistencyReachedLast126, last126EuroConsistency);

            // Consistency 1Y        
            cdiConsistencyReachedLast252 = calcConsistencyForPeriod(ird_investment_return_1y, ird_cdi_investment_return_1y, 252, cdiConsistencyReachedLast252, last252CDIConsistency);
            ird_cdi_consistency_1y = getConsistencyForPeriod(cdiConsistencyReachedLast252, last252CDIConsistency);
            bovespaConsistencyReachedLast252 = calcConsistencyForPeriod(ird_investment_return_1y, ird_bovespa_investment_return_1y, 252, bovespaConsistencyReachedLast252, last252BovespaConsistency);
            ird_bovespa_consistency_1y = getConsistencyForPeriod(bovespaConsistencyReachedLast252, last252BovespaConsistency);
            ipcaConsistencyReachedLast252 = calcConsistencyForPeriod(ird_investment_return_1y, ird_ipca_investment_return_1y, 252, ipcaConsistencyReachedLast252, last252IPCAConsistency);
            ird_ipca_consistency_1y = getConsistencyForPeriod(ipcaConsistencyReachedLast252, last252IPCAConsistency);
            igpmConsistencyReachedLast252 = calcConsistencyForPeriod(ird_investment_return_1y, ird_igpm_investment_return_1y, 252, igpmConsistencyReachedLast252, last252IGPMConsistency);
            ird_igpm_consistency_1y = getConsistencyForPeriod(igpmConsistencyReachedLast252, last252IGPMConsistency);
            igpdiConsistencyReachedLast252 = calcConsistencyForPeriod(ird_investment_return_1y, ird_igpdi_investment_return_1y, 252, igpdiConsistencyReachedLast252, last252IGPDIConsistency);
            ird_igpdi_consistency_1y = getConsistencyForPeriod(igpdiConsistencyReachedLast252, last252IGPDIConsistency);
            dolarConsistencyReachedLast252 = calcConsistencyForPeriod(ird_investment_return_1y, ird_dolar_investment_return_1y, 252, dolarConsistencyReachedLast252, last252DolarConsistency);
            ird_dolar_consistency_1y = getConsistencyForPeriod(dolarConsistencyReachedLast252, last252DolarConsistency);
            euroConsistencyReachedLast252 = calcConsistencyForPeriod(ird_investment_return_1y, ird_euro_investment_return_1y, 252, euroConsistencyReachedLast252, last252EuroConsistency);
            ird_euro_consistency_1y = getConsistencyForPeriod(euroConsistencyReachedLast252, last252EuroConsistency);

            // Consistency 2Y
            cdiConsistencyReachedLast504 = calcConsistencyForPeriod(ird_investment_return_2y, ird_cdi_investment_return_2y, 504, cdiConsistencyReachedLast504, last504CDIConsistency);
            ird_cdi_consistency_2y = getConsistencyForPeriod(cdiConsistencyReachedLast504, last504CDIConsistency);
            bovespaConsistencyReachedLast504 = calcConsistencyForPeriod(ird_investment_return_2y, ird_bovespa_investment_return_2y, 504, bovespaConsistencyReachedLast504, last504BovespaConsistency);
            ird_bovespa_consistency_2y = getConsistencyForPeriod(bovespaConsistencyReachedLast504, last504BovespaConsistency);
            ipcaConsistencyReachedLast504 = calcConsistencyForPeriod(ird_investment_return_2y, ird_ipca_investment_return_2y, 504, ipcaConsistencyReachedLast504, last504IPCAConsistency);
            ird_ipca_consistency_2y = getConsistencyForPeriod(ipcaConsistencyReachedLast504, last504IPCAConsistency);
            igpmConsistencyReachedLast504 = calcConsistencyForPeriod(ird_investment_return_2y, ird_igpm_investment_return_2y, 504, igpmConsistencyReachedLast504, last504IGPMConsistency);
            ird_igpm_consistency_2y = getConsistencyForPeriod(igpmConsistencyReachedLast504, last504IGPMConsistency);
            igpdiConsistencyReachedLast504 = calcConsistencyForPeriod(ird_investment_return_2y, ird_igpdi_investment_return_2y, 504, igpdiConsistencyReachedLast504, last504IGPDIConsistency);
            ird_igpdi_consistency_2y = getConsistencyForPeriod(igpdiConsistencyReachedLast504, last504IGPDIConsistency);
            dolarConsistencyReachedLast504 = calcConsistencyForPeriod(ird_investment_return_2y, ird_dolar_investment_return_2y, 504, dolarConsistencyReachedLast504, last504DolarConsistency);
            ird_dolar_consistency_2y = getConsistencyForPeriod(dolarConsistencyReachedLast504, last504DolarConsistency);
            euroConsistencyReachedLast504 = calcConsistencyForPeriod(ird_investment_return_2y, ird_euro_investment_return_2y, 504, euroConsistencyReachedLast504, last504EuroConsistency);
            ird_euro_consistency_2y = getConsistencyForPeriod(euroConsistencyReachedLast504, last504EuroConsistency);

            // Consistency 3Y
            cdiConsistencyReachedLast756 = calcConsistencyForPeriod(ird_investment_return_3y, ird_cdi_investment_return_3y, 756, cdiConsistencyReachedLast756, last756CDIConsistency);
            ird_cdi_consistency_3y = getConsistencyForPeriod(cdiConsistencyReachedLast756, last756CDIConsistency);
            bovespaConsistencyReachedLast756 = calcConsistencyForPeriod(ird_investment_return_3y, ird_bovespa_investment_return_3y, 756, bovespaConsistencyReachedLast756, last756BovespaConsistency);
            ird_bovespa_consistency_3y = getConsistencyForPeriod(bovespaConsistencyReachedLast756, last756BovespaConsistency);
            ipcaConsistencyReachedLast756 = calcConsistencyForPeriod(ird_investment_return_3y, ird_ipca_investment_return_3y, 756, ipcaConsistencyReachedLast756, last756IPCAConsistency);
            ird_ipca_consistency_3y = getConsistencyForPeriod(ipcaConsistencyReachedLast756, last756IPCAConsistency);
            igpmConsistencyReachedLast756 = calcConsistencyForPeriod(ird_investment_return_3y, ird_igpm_investment_return_3y, 756, igpmConsistencyReachedLast756, last756IGPMConsistency);
            ird_igpm_consistency_3y = getConsistencyForPeriod(igpmConsistencyReachedLast756, last756IGPMConsistency);
            igpdiConsistencyReachedLast756 = calcConsistencyForPeriod(ird_investment_return_3y, ird_igpdi_investment_return_3y, 756, igpdiConsistencyReachedLast756, last756IGPDIConsistency);
            ird_igpdi_consistency_3y = getConsistencyForPeriod(igpdiConsistencyReachedLast756, last756IGPDIConsistency);
            dolarConsistencyReachedLast756 = calcConsistencyForPeriod(ird_investment_return_3y, ird_dolar_investment_return_3y, 756, dolarConsistencyReachedLast756, last756DolarConsistency);
            ird_dolar_consistency_3y = getConsistencyForPeriod(dolarConsistencyReachedLast756, last756DolarConsistency);
            euroConsistencyReachedLast756 = calcConsistencyForPeriod(ird_investment_return_3y, ird_euro_investment_return_3y, 756, euroConsistencyReachedLast756, last756EuroConsistency);
            ird_euro_consistency_3y = getConsistencyForPeriod(euroConsistencyReachedLast756, last756EuroConsistency);

            // Networth
            ird_networth = returnFromQuota(chunkBefore.vl_patrim_liq, chunk.vl_patrim_liq);
            ird_networth_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].vl_patrim_liq, chunk.vl_patrim_liq);
            ird_networth_ytd = returnFromQuota(chunksByYear[year][0].vl_patrim_liq, chunk.vl_patrim_liq);
            ird_networth_1m = returnFromQuota(last21Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            ird_networth_3m = returnFromQuota(last63Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            ird_networth_6m = returnFromQuota(last126Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            ird_networth_1y = returnFromQuota(last252Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            ird_networth_2y = returnFromQuota(last504Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            ird_networth_3y = returnFromQuota(last756Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            ird_accumulated_networth = chunk.vl_patrim_liq;

            // Quotaholders
            ird_quotaholders = returnFromQuota(chunkBefore.nr_cotst, chunk.nr_cotst);
            ird_quotaholders_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].nr_cotst, chunk.nr_cotst);
            ird_quotaholders_ytd = returnFromQuota(chunksByYear[year][0].nr_cotst, chunk.nr_cotst);
            ird_quotaholders_1m = returnFromQuota(last21Chunks[0].nr_cotst, chunk.nr_cotst);
            ird_quotaholders_3m = returnFromQuota(last63Chunks[0].nr_cotst, chunk.nr_cotst);
            ird_quotaholders_6m = returnFromQuota(last126Chunks[0].nr_cotst, chunk.nr_cotst);
            ird_quotaholders_1y = returnFromQuota(last252Chunks[0].nr_cotst, chunk.nr_cotst);
            ird_quotaholders_2y = returnFromQuota(last504Chunks[0].nr_cotst, chunk.nr_cotst);
            ird_quotaholders_3y = returnFromQuota(last756Chunks[0].nr_cotst, chunk.nr_cotst);
            ird_accumulated_quotaholders = chunk.nr_cotst;
        }

        const ird_dt_comptc = `${chunk.dt_comptc.$y}-${string.pad(chunk.dt_comptc.$M + 1, 2)}-${string.pad(chunk.dt_comptc.$D, 2)}`;

        if (startSavingFrom == null || ird_dt_comptc >= startSavingFrom) {
            stream.push({
                table: 'investment_return_daily',
                primaryKey: ['ird_CNPJ_FUNDO', 'ird_DT_COMPTC'],
                fields: {
                    ird_id: uuidv1(),
                    ird_cnpj_fundo: chunk.cnpj_fundo,
                    ird_dt_comptc: ird_dt_comptc,
                    ird_investment_return,
                    ird_cdi_investment_return,
                    ird_bovespa_investment_return,
                    ird_ipca_investment_return,
                    ird_igpm_investment_return,
                    ird_igpdi_investment_return,
                    ird_dolar_investment_return,
                    ird_euro_investment_return,
                    ird_investment_return_mtd,
                    ird_cdi_investment_return_mtd,
                    ird_bovespa_investment_return_mtd,
                    ird_ipca_investment_return_mtd,
                    ird_igpm_investment_return_mtd,
                    ird_igpdi_investment_return_mtd,
                    ird_dolar_investment_return_mtd,
                    ird_euro_investment_return_mtd,
                    ird_investment_return_ytd,
                    ird_cdi_investment_return_ytd,
                    ird_bovespa_investment_return_ytd,
                    ird_ipca_investment_return_ytd,
                    ird_igpm_investment_return_ytd,
                    ird_igpdi_investment_return_ytd,
                    ird_dolar_investment_return_ytd,
                    ird_euro_investment_return_ytd,
                    ird_investment_return_1m,
                    ird_cdi_investment_return_1m,
                    ird_bovespa_investment_return_1m,
                    ird_ipca_investment_return_1m,
                    ird_igpm_investment_return_1m,
                    ird_igpdi_investment_return_1m,
                    ird_dolar_investment_return_1m,
                    ird_euro_investment_return_1m,
                    ird_investment_return_3m,
                    ird_cdi_investment_return_3m,
                    ird_bovespa_investment_return_3m,
                    ird_ipca_investment_return_3m,
                    ird_igpm_investment_return_3m,
                    ird_igpdi_investment_return_3m,
                    ird_dolar_investment_return_3m,
                    ird_euro_investment_return_3m,
                    ird_investment_return_6m,
                    ird_cdi_investment_return_6m,
                    ird_bovespa_investment_return_6m,
                    ird_ipca_investment_return_6m,
                    ird_igpm_investment_return_6m,
                    ird_igpdi_investment_return_6m,
                    ird_dolar_investment_return_6m,
                    ird_euro_investment_return_6m,
                    ird_investment_return_1y,
                    ird_cdi_investment_return_1y,
                    ird_bovespa_investment_return_1y,
                    ird_ipca_investment_return_1y,
                    ird_igpm_investment_return_1y,
                    ird_igpdi_investment_return_1y,
                    ird_dolar_investment_return_1y,
                    ird_euro_investment_return_1y,
                    ird_investment_return_2y,
                    ird_cdi_investment_return_2y,
                    ird_bovespa_investment_return_2y,
                    ird_ipca_investment_return_2y,
                    ird_igpm_investment_return_2y,
                    ird_igpdi_investment_return_2y,
                    ird_dolar_investment_return_2y,
                    ird_euro_investment_return_2y,
                    ird_investment_return_3y,
                    ird_cdi_investment_return_3y,
                    ird_bovespa_investment_return_3y,
                    ird_ipca_investment_return_3y,
                    ird_igpm_investment_return_3y,
                    ird_igpdi_investment_return_3y,
                    ird_dolar_investment_return_3y,
                    ird_euro_investment_return_3y,
                    ird_accumulated_investment_return,
                    ird_cdi_accumulated_investment_return,
                    ird_bovespa_accumulated_investment_return,
                    ird_ipca_accumulated_investment_return,
                    ird_igpm_accumulated_investment_return,
                    ird_igpdi_accumulated_investment_return,
                    ird_dolar_accumulated_investment_return,
                    ird_euro_accumulated_investment_return,
                    ird_risk_mtd,
                    ird_risk_ytd,
                    ird_risk_1m,
                    ird_risk_3m,
                    ird_risk_6m,
                    ird_risk_1y,
                    ird_risk_2y,
                    ird_risk_3y,
                    ird_accumulated_risk,
                    ird_cdi_sharpe_mtd,
                    ird_bovespa_sharpe_mtd,
                    ird_ipca_sharpe_mtd,
                    ird_igpm_sharpe_mtd,
                    ird_igpdi_sharpe_mtd,
                    ird_dolar_sharpe_mtd,
                    ird_euro_sharpe_mtd,
                    ird_cdi_sharpe_ytd,
                    ird_bovespa_sharpe_ytd,
                    ird_ipca_sharpe_ytd,
                    ird_igpm_sharpe_ytd,
                    ird_igpdi_sharpe_ytd,
                    ird_dolar_sharpe_ytd,
                    ird_euro_sharpe_ytd,
                    ird_cdi_sharpe_1m,
                    ird_bovespa_sharpe_1m,
                    ird_ipca_sharpe_1m,
                    ird_igpm_sharpe_1m,
                    ird_igpdi_sharpe_1m,
                    ird_dolar_sharpe_1m,
                    ird_euro_sharpe_1m,
                    ird_cdi_sharpe_3m,
                    ird_bovespa_sharpe_3m,
                    ird_ipca_sharpe_3m,
                    ird_igpm_sharpe_3m,
                    ird_igpdi_sharpe_3m,
                    ird_dolar_sharpe_3m,
                    ird_euro_sharpe_3m,
                    ird_cdi_sharpe_6m,
                    ird_bovespa_sharpe_6m,
                    ird_ipca_sharpe_6m,
                    ird_igpm_sharpe_6m,
                    ird_igpdi_sharpe_6m,
                    ird_dolar_sharpe_6m,
                    ird_euro_sharpe_6m,
                    ird_cdi_sharpe_1y,
                    ird_bovespa_sharpe_1y,
                    ird_ipca_sharpe_1y,
                    ird_igpm_sharpe_1y,
                    ird_igpdi_sharpe_1y,
                    ird_dolar_sharpe_1y,
                    ird_euro_sharpe_1y,
                    ird_cdi_sharpe_2y,
                    ird_bovespa_sharpe_2y,
                    ird_ipca_sharpe_2y,
                    ird_igpm_sharpe_2y,
                    ird_igpdi_sharpe_2y,
                    ird_dolar_sharpe_2y,
                    ird_euro_sharpe_2y,
                    ird_cdi_sharpe_3y,
                    ird_bovespa_sharpe_3y,
                    ird_ipca_sharpe_3y,
                    ird_igpm_sharpe_3y,
                    ird_igpdi_sharpe_3y,
                    ird_dolar_sharpe_3y,
                    ird_euro_sharpe_3y,
                    ird_cdi_accumulated_sharpe,
                    ird_bovespa_accumulated_sharpe,
                    ird_ipca_accumulated_sharpe,
                    ird_igpm_accumulated_sharpe,
                    ird_igpdi_accumulated_sharpe,
                    ird_dolar_accumulated_sharpe,
                    ird_euro_accumulated_sharpe,
                    ird_cdi_consistency_mtd,
                    ird_bovespa_consistency_mtd,
                    ird_ipca_consistency_mtd,
                    ird_igpm_consistency_mtd,
                    ird_igpdi_consistency_mtd,
                    ird_dolar_consistency_mtd,
                    ird_euro_consistency_mtd,
                    ird_cdi_consistency_ytd,
                    ird_bovespa_consistency_ytd,
                    ird_ipca_consistency_ytd,
                    ird_igpm_consistency_ytd,
                    ird_igpdi_consistency_ytd,
                    ird_dolar_consistency_ytd,
                    ird_euro_consistency_ytd,
                    ird_cdi_consistency_1m,
                    ird_bovespa_consistency_1m,
                    ird_ipca_consistency_1m,
                    ird_igpm_consistency_1m,
                    ird_igpdi_consistency_1m,
                    ird_dolar_consistency_1m,
                    ird_euro_consistency_1m,
                    ird_cdi_consistency_3m,
                    ird_bovespa_consistency_3m,
                    ird_ipca_consistency_3m,
                    ird_igpm_consistency_3m,
                    ird_igpdi_consistency_3m,
                    ird_dolar_consistency_3m,
                    ird_euro_consistency_3m,
                    ird_cdi_consistency_6m,
                    ird_bovespa_consistency_6m,
                    ird_ipca_consistency_6m,
                    ird_igpm_consistency_6m,
                    ird_igpdi_consistency_6m,
                    ird_dolar_consistency_6m,
                    ird_euro_consistency_6m,
                    ird_cdi_consistency_1y,
                    ird_bovespa_consistency_1y,
                    ird_ipca_consistency_1y,
                    ird_igpm_consistency_1y,
                    ird_igpdi_consistency_1y,
                    ird_dolar_consistency_1y,
                    ird_euro_consistency_1y,
                    ird_cdi_consistency_2y,
                    ird_bovespa_consistency_2y,
                    ird_ipca_consistency_2y,
                    ird_igpm_consistency_2y,
                    ird_igpdi_consistency_2y,
                    ird_dolar_consistency_2y,
                    ird_euro_consistency_2y,
                    ird_cdi_consistency_3y,
                    ird_bovespa_consistency_3y,
                    ird_ipca_consistency_3y,
                    ird_igpm_consistency_3y,
                    ird_igpdi_consistency_3y,
                    ird_dolar_consistency_3y,
                    ird_euro_consistency_3y,
                    ird_networth,
                    ird_accumulated_networth,
                    ird_networth_mtd,
                    ird_networth_ytd,
                    ird_networth_1m,
                    ird_networth_3m,
                    ird_networth_6m,
                    ird_networth_1y,
                    ird_networth_2y,
                    ird_networth_3y,
                    ird_quotaholders,
                    ird_accumulated_quotaholders,
                    ird_quotaholders_mtd,
                    ird_quotaholders_ytd,
                    ird_quotaholders_1m,
                    ird_quotaholders_3m,
                    ird_quotaholders_6m,
                    ird_quotaholders_1y,
                    ird_quotaholders_2y,
                    ird_quotaholders_3y
                }
            });
        }
    };

    const processMonthly = (stream, chunk) => {
        const monthBefore = chunk.dt_comptc.subtract(1, 'month');
        const lastMonth = getMonthHash(monthBefore);
        const lastYear = getYearHash(monthBefore);
        const month = getMonthHash(chunk.dt_comptc);
        const year = getYearHash(chunk.dt_comptc);

        if (risk) {
            // Return            
            const irm_investment_return = returnFromQuota(lastMonthOrFirstDay(chunk, year, month, lastYear, lastMonth).vl_quota, chunk.vl_quota);
            const irm_cdi_investment_return = returnFromQuota(lastMonthOrFirstDay(chunk, year, month, lastYear, lastMonth).cdi_quota, chunk.cdi_quota);
            const irm_bovespa_investment_return = returnFromQuota(lastMonthOrFirstDay(chunk, year, month, lastYear, lastMonth).bovespa_valor, chunk.bovespa_valor);
            const irm_ipca_investment_return = returnFromQuota(lastMonthOrFirstDay(chunk, year, month, lastYear, lastMonth).ipca_quota, chunk.ipca_quota);
            const irm_igpm_investment_return = returnFromQuota(lastMonthOrFirstDay(chunk, year, month, lastYear, lastMonth).igpm_quota, chunk.igpm_quota);
            const irm_igpdi_investment_return = returnFromQuota(lastMonthOrFirstDay(chunk, year, month, lastYear, lastMonth).igpdi_quota, chunk.igpdi_quota);
            const irm_dolar_investment_return = returnFromQuota(lastMonthOrFirstDay(chunk, year, month, lastYear, lastMonth).dolar_valor, chunk.dolar_valor);
            const irm_euro_investment_return = returnFromQuota(lastMonthOrFirstDay(chunk, year, month, lastYear, lastMonth).euro_valor, chunk.euro_valor);

            // Return MTD
            const irm_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].vl_quota, chunk.vl_quota);
            const irm_cdi_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].bovespa_valor, chunk.bovespa_valor);
            const irm_ipca_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].ipca_quota, chunk.ipca_quota);
            const irm_igpm_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].igpm_quota, chunk.igpm_quota);
            const irm_igpdi_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].igpdi_quota, chunk.igpdi_quota);
            const irm_dolar_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].dolar_valor, chunk.dolar_valor);
            const irm_euro_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].euro_valor, chunk.euro_valor);

            // Return YTD
            const irm_investment_return_ytd = returnFromQuota(chunksByYear[year][0].vl_quota, chunk.vl_quota);
            const irm_cdi_investment_return_ytd = returnFromQuota(chunksByYear[year][0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_investment_return_ytd = returnFromQuota(chunksByYear[year][0].bovespa_valor, chunk.bovespa_valor);
            const irm_ipca_investment_return_ytd = returnFromQuota(chunksByYear[year][0].ipca_quota, chunk.ipca_quota);
            const irm_igpm_investment_return_ytd = returnFromQuota(chunksByYear[year][0].igpm_quota, chunk.igpm_quota);
            const irm_igpdi_investment_return_ytd = returnFromQuota(chunksByYear[year][0].igpdi_quota, chunk.igpdi_quota);
            const irm_dolar_investment_return_ytd = returnFromQuota(chunksByYear[year][0].dolar_valor, chunk.dolar_valor);
            const irm_euro_investment_return_ytd = returnFromQuota(chunksByYear[year][0].euro_valor, chunk.euro_valor);

            // Return 1M
            const irm_investment_return_1m = returnFromQuota(last21Chunks[0].vl_quota, chunk.vl_quota);
            const irm_cdi_investment_return_1m = returnFromQuota(last21Chunks[0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_investment_return_1m = returnFromQuota(last21Chunks[0].bovespa_valor, chunk.bovespa_valor);
            const irm_ipca_investment_return_1m = returnFromQuota(last21Chunks[0].ipca_quota, chunk.ipca_quota);
            const irm_igpm_investment_return_1m = returnFromQuota(last21Chunks[0].igpm_quota, chunk.igpm_quota);
            const irm_igpdi_investment_return_1m = returnFromQuota(last21Chunks[0].igpdi_quota, chunk.igpdi_quota);
            const irm_dolar_investment_return_1m = returnFromQuota(last21Chunks[0].dolar_valor, chunk.dolar_valor);
            const irm_euro_investment_return_1m = returnFromQuota(last21Chunks[0].euro_valor, chunk.euro_valor);

            // Return 3M
            const irm_investment_return_3m = returnFromQuota(last63Chunks[0].vl_quota, chunk.vl_quota);
            const irm_cdi_investment_return_3m = returnFromQuota(last63Chunks[0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_investment_return_3m = returnFromQuota(last63Chunks[0].bovespa_valor, chunk.bovespa_valor);
            const irm_ipca_investment_return_3m = returnFromQuota(last63Chunks[0].ipca_quota, chunk.ipca_quota);
            const irm_igpm_investment_return_3m = returnFromQuota(last63Chunks[0].igpm_quota, chunk.igpm_quota);
            const irm_igpdi_investment_return_3m = returnFromQuota(last63Chunks[0].igpdi_quota, chunk.igpdi_quota);
            const irm_dolar_investment_return_3m = returnFromQuota(last63Chunks[0].dolar_valor, chunk.dolar_valor);
            const irm_euro_investment_return_3m = returnFromQuota(last63Chunks[0].euro_valor, chunk.euro_valor);

            // Return 6M
            const irm_investment_return_6m = returnFromQuota(last126Chunks[0].vl_quota, chunk.vl_quota);
            const irm_cdi_investment_return_6m = returnFromQuota(last126Chunks[0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_investment_return_6m = returnFromQuota(last126Chunks[0].bovespa_valor, chunk.bovespa_valor);
            const irm_ipca_investment_return_6m = returnFromQuota(last126Chunks[0].ipca_quota, chunk.ipca_quota);
            const irm_igpm_investment_return_6m = returnFromQuota(last126Chunks[0].igpm_quota, chunk.igpm_quota);
            const irm_igpdi_investment_return_6m = returnFromQuota(last126Chunks[0].igpdi_quota, chunk.igpdi_quota);
            const irm_dolar_investment_return_6m = returnFromQuota(last126Chunks[0].dolar_valor, chunk.dolar_valor);
            const irm_euro_investment_return_6m = returnFromQuota(last126Chunks[0].euro_valor, chunk.euro_valor);

            // Return 1Y            
            const irm_investment_return_1y = returnFromQuota(last252Chunks[0].vl_quota, chunk.vl_quota);
            const irm_cdi_investment_return_1y = returnFromQuota(last252Chunks[0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_investment_return_1y = returnFromQuota(last252Chunks[0].bovespa_valor, chunk.bovespa_valor);
            const irm_ipca_investment_return_1y = returnFromQuota(last252Chunks[0].ipca_quota, chunk.ipca_quota);
            const irm_igpm_investment_return_1y = returnFromQuota(last252Chunks[0].igpm_quota, chunk.igpm_quota);
            const irm_igpdi_investment_return_1y = returnFromQuota(last252Chunks[0].igpdi_quota, chunk.igpdi_quota);
            const irm_dolar_investment_return_1y = returnFromQuota(last252Chunks[0].dolar_valor, chunk.dolar_valor);
            const irm_euro_investment_return_1y = returnFromQuota(last252Chunks[0].euro_valor, chunk.euro_valor);

            // Return 2Y
            const irm_investment_return_2y = returnFromQuota(last504Chunks[0].vl_quota, chunk.vl_quota);
            const irm_cdi_investment_return_2y = returnFromQuota(last504Chunks[0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_investment_return_2y = returnFromQuota(last504Chunks[0].bovespa_valor, chunk.bovespa_valor);
            const irm_ipca_investment_return_2y = returnFromQuota(last504Chunks[0].ipca_quota, chunk.ipca_quota);
            const irm_igpm_investment_return_2y = returnFromQuota(last504Chunks[0].igpm_quota, chunk.igpm_quota);
            const irm_igpdi_investment_return_2y = returnFromQuota(last504Chunks[0].igpdi_quota, chunk.igpdi_quota);
            const irm_dolar_investment_return_2y = returnFromQuota(last504Chunks[0].dolar_valor, chunk.dolar_valor);
            const irm_euro_investment_return_2y = returnFromQuota(last504Chunks[0].euro_valor, chunk.euro_valor);

            // Return 3Y
            const irm_investment_return_3y = returnFromQuota(last756Chunks[0].vl_quota, chunk.vl_quota);
            const irm_cdi_investment_return_3y = returnFromQuota(last756Chunks[0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_investment_return_3y = returnFromQuota(last756Chunks[0].bovespa_valor, chunk.bovespa_valor);
            const irm_ipca_investment_return_3y = returnFromQuota(last756Chunks[0].ipca_quota, chunk.ipca_quota);
            const irm_igpm_investment_return_3y = returnFromQuota(last756Chunks[0].igpm_quota, chunk.igpm_quota);
            const irm_igpdi_investment_return_3y = returnFromQuota(last756Chunks[0].igpdi_quota, chunk.igpdi_quota);
            const irm_dolar_investment_return_3y = returnFromQuota(last756Chunks[0].dolar_valor, chunk.dolar_valor);
            const irm_euro_investment_return_3y = returnFromQuota(last756Chunks[0].euro_valor, chunk.euro_valor);

            // Accumulated Return
            const irm_accumulated_investment_return = returnFromQuota(chunks[0].vl_quota, chunk.vl_quota);
            const irm_cdi_accumulated_investment_return = returnFromQuota(chunks[0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_accumulated_investment_return = returnFromQuota(chunks[0].bovespa_valor, chunk.bovespa_valor);
            const irm_ipca_accumulated_investment_return = returnFromQuota(chunks[0].ipca_quota, chunk.ipca_quota);
            const irm_igpm_accumulated_investment_return = returnFromQuota(chunks[0].igpm_quota, chunk.igpm_quota);
            const irm_igpdi_accumulated_investment_return = returnFromQuota(chunks[0].igpdi_quota, chunk.igpdi_quota);
            const irm_dolar_accumulated_investment_return = returnFromQuota(chunks[0].dolar_valor, chunk.dolar_valor);
            const irm_euro_accumulated_investment_return = returnFromQuota(chunks[0].euro_valor, chunk.euro_valor);

            // Months Risk            
            const irm_risk = risksByMonth[month].get() * Math.sqrt(252);

            // Risk MTD
            const irm_risk_mtd = risksByMonth[month].get() * Math.sqrt(252);

            // Risk YTD
            const irm_risk_ytd = risksByYear[year].get() * Math.sqrt(252);

            // Risk 1M
            const irm_risk_1m = last21Risk.get() * Math.sqrt(252);

            // Risk 3M
            const irm_risk_3m = last63Risk.get() * Math.sqrt(252);

            // Risk 6M
            const irm_risk_6m = last126Risk.get() * Math.sqrt(252);

            // Risk 1Y            
            const irm_risk_1y = last252Risk.get() * Math.sqrt(252);

            // Risk 2Y            
            const irm_risk_2y = last504Risk.get() * Math.sqrt(252);

            // Risk 3Y            
            const irm_risk_3y = last756Risk.get() * Math.sqrt(252);

            // Accumulated Risk
            const irm_accumulated_risk = risk.get() * Math.sqrt(252);

            // Sharpe
            const irm_cdi_sharpe = calcSharpeForPeriod(irm_risk, irm_investment_return, irm_cdi_investment_return, chunksByYearAndMonth[year][month].length - 1);
            const irm_bovespa_sharpe = calcSharpeForPeriod(irm_risk, irm_investment_return, irm_bovespa_investment_return, chunksByYearAndMonth[year][month].length - 1);
            const irm_ipca_sharpe = calcSharpeForPeriod(irm_risk, irm_investment_return, irm_ipca_investment_return, chunksByYearAndMonth[year][month].length - 1);
            const irm_igpm_sharpe = calcSharpeForPeriod(irm_risk, irm_investment_return, irm_igpm_investment_return, chunksByYearAndMonth[year][month].length - 1);
            const irm_igpdi_sharpe = calcSharpeForPeriod(irm_risk, irm_investment_return, irm_igpdi_investment_return, chunksByYearAndMonth[year][month].length - 1);
            const irm_dolar_sharpe = calcSharpeForPeriod(irm_risk, irm_investment_return, irm_dolar_investment_return, chunksByYearAndMonth[year][month].length - 1);
            const irm_euro_sharpe = calcSharpeForPeriod(irm_risk, irm_investment_return, irm_euro_investment_return, chunksByYearAndMonth[year][month].length - 1);

            // Sharpe MTD
            const irm_cdi_sharpe_mtd = calcSharpeForPeriod(irm_risk_mtd, irm_investment_return_mtd, irm_cdi_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            const irm_bovespa_sharpe_mtd = calcSharpeForPeriod(irm_risk_mtd, irm_investment_return_mtd, irm_bovespa_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            const irm_ipca_sharpe_mtd = calcSharpeForPeriod(irm_risk_mtd, irm_investment_return_mtd, irm_ipca_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            const irm_igpm_sharpe_mtd = calcSharpeForPeriod(irm_risk_mtd, irm_investment_return_mtd, irm_igpm_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            const irm_igpdi_sharpe_mtd = calcSharpeForPeriod(irm_risk_mtd, irm_investment_return_mtd, irm_igpdi_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            const irm_dolar_sharpe_mtd = calcSharpeForPeriod(irm_risk_mtd, irm_investment_return_mtd, irm_dolar_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            const irm_euro_sharpe_mtd = calcSharpeForPeriod(irm_risk_mtd, irm_investment_return_mtd, irm_euro_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);

            // Sharpe YTD
            const irm_cdi_sharpe_ytd = calcSharpeForPeriod(irm_risk_ytd, irm_investment_return_ytd, irm_cdi_investment_return_ytd, chunksByYear[year].length - 1);
            const irm_bovespa_sharpe_ytd = calcSharpeForPeriod(irm_risk_ytd, irm_investment_return_ytd, irm_bovespa_investment_return_ytd, chunksByYear[year].length - 1);
            const irm_ipca_sharpe_ytd = calcSharpeForPeriod(irm_risk_ytd, irm_investment_return_ytd, irm_ipca_investment_return_ytd, chunksByYear[year].length - 1);
            const irm_igpm_sharpe_ytd = calcSharpeForPeriod(irm_risk_ytd, irm_investment_return_ytd, irm_igpm_investment_return_ytd, chunksByYear[year].length - 1);
            const irm_igpdi_sharpe_ytd = calcSharpeForPeriod(irm_risk_ytd, irm_investment_return_ytd, irm_igpdi_investment_return_ytd, chunksByYear[year].length - 1);
            const irm_dolar_sharpe_ytd = calcSharpeForPeriod(irm_risk_ytd, irm_investment_return_ytd, irm_dolar_investment_return_ytd, chunksByYear[year].length - 1);
            const irm_euro_sharpe_ytd = calcSharpeForPeriod(irm_risk_ytd, irm_investment_return_ytd, irm_euro_investment_return_ytd, chunksByYear[year].length - 1);

            // Sharpe 1M
            const irm_cdi_sharpe_1m = calcSharpeForPeriod(irm_risk_1m, irm_investment_return_1m, irm_cdi_investment_return_1m, last21Chunks.length - 1);
            const irm_bovespa_sharpe_1m = calcSharpeForPeriod(irm_risk_1m, irm_investment_return_1m, irm_bovespa_investment_return_1m, last21Chunks.length - 1);
            const irm_ipca_sharpe_1m = calcSharpeForPeriod(irm_risk_1m, irm_investment_return_1m, irm_ipca_investment_return_1m, last21Chunks.length - 1);
            const irm_igpm_sharpe_1m = calcSharpeForPeriod(irm_risk_1m, irm_investment_return_1m, irm_igpm_investment_return_1m, last21Chunks.length - 1);
            const irm_igpdi_sharpe_1m = calcSharpeForPeriod(irm_risk_1m, irm_investment_return_1m, irm_igpdi_investment_return_1m, last21Chunks.length - 1);
            const irm_dolar_sharpe_1m = calcSharpeForPeriod(irm_risk_1m, irm_investment_return_1m, irm_dolar_investment_return_1m, last21Chunks.length - 1);
            const irm_euro_sharpe_1m = calcSharpeForPeriod(irm_risk_1m, irm_investment_return_1m, irm_euro_investment_return_1m, last21Chunks.length - 1);

            // Sharpe 3M
            const irm_cdi_sharpe_3m = calcSharpeForPeriod(irm_risk_3m, irm_investment_return_3m, irm_cdi_investment_return_3m, last63Chunks.length - 1);
            const irm_bovespa_sharpe_3m = calcSharpeForPeriod(irm_risk_3m, irm_investment_return_3m, irm_bovespa_investment_return_3m, last63Chunks.length - 1);
            const irm_ipca_sharpe_3m = calcSharpeForPeriod(irm_risk_3m, irm_investment_return_3m, irm_ipca_investment_return_3m, last63Chunks.length - 1);
            const irm_igpm_sharpe_3m = calcSharpeForPeriod(irm_risk_3m, irm_investment_return_3m, irm_igpm_investment_return_3m, last63Chunks.length - 1);
            const irm_igpdi_sharpe_3m = calcSharpeForPeriod(irm_risk_3m, irm_investment_return_3m, irm_igpdi_investment_return_3m, last63Chunks.length - 1);
            const irm_dolar_sharpe_3m = calcSharpeForPeriod(irm_risk_3m, irm_investment_return_3m, irm_dolar_investment_return_3m, last63Chunks.length - 1);
            const irm_euro_sharpe_3m = calcSharpeForPeriod(irm_risk_3m, irm_investment_return_3m, irm_euro_investment_return_3m, last63Chunks.length - 1);

            // Sharpe 6M
            const irm_cdi_sharpe_6m = calcSharpeForPeriod(irm_risk_6m, irm_investment_return_6m, irm_cdi_investment_return_6m, last126Chunks.length - 1);
            const irm_bovespa_sharpe_6m = calcSharpeForPeriod(irm_risk_6m, irm_investment_return_6m, irm_bovespa_investment_return_6m, last126Chunks.length - 1);
            const irm_ipca_sharpe_6m = calcSharpeForPeriod(irm_risk_6m, irm_investment_return_6m, irm_ipca_investment_return_6m, last126Chunks.length - 1);
            const irm_igpm_sharpe_6m = calcSharpeForPeriod(irm_risk_6m, irm_investment_return_6m, irm_igpm_investment_return_6m, last126Chunks.length - 1);
            const irm_igpdi_sharpe_6m = calcSharpeForPeriod(irm_risk_6m, irm_investment_return_6m, irm_igpdi_investment_return_6m, last126Chunks.length - 1);
            const irm_dolar_sharpe_6m = calcSharpeForPeriod(irm_risk_6m, irm_investment_return_6m, irm_dolar_investment_return_6m, last126Chunks.length - 1);
            const irm_euro_sharpe_6m = calcSharpeForPeriod(irm_risk_6m, irm_investment_return_6m, irm_euro_investment_return_6m, last126Chunks.length - 1);

            // Sharpe 1Y
            const irm_cdi_sharpe_1y = calcSharpeForPeriod(irm_risk_1y, irm_investment_return_1y, irm_cdi_investment_return_1y, last252Chunks.length - 1);
            const irm_bovespa_sharpe_1y = calcSharpeForPeriod(irm_risk_1y, irm_investment_return_1y, irm_bovespa_investment_return_1y, last252Chunks.length - 1);
            const irm_ipca_sharpe_1y = calcSharpeForPeriod(irm_risk_1y, irm_investment_return_1y, irm_ipca_investment_return_1y, last252Chunks.length - 1);
            const irm_igpm_sharpe_1y = calcSharpeForPeriod(irm_risk_1y, irm_investment_return_1y, irm_igpm_investment_return_1y, last252Chunks.length - 1);
            const irm_igpdi_sharpe_1y = calcSharpeForPeriod(irm_risk_1y, irm_investment_return_1y, irm_igpdi_investment_return_1y, last252Chunks.length - 1);
            const irm_dolar_sharpe_1y = calcSharpeForPeriod(irm_risk_1y, irm_investment_return_1y, irm_dolar_investment_return_1y, last252Chunks.length - 1);
            const irm_euro_sharpe_1y = calcSharpeForPeriod(irm_risk_1y, irm_investment_return_1y, irm_euro_investment_return_1y, last252Chunks.length - 1);

            // Sharpe 2Y
            const irm_cdi_sharpe_2y = calcSharpeForPeriod(irm_risk_2y, irm_investment_return_2y, irm_cdi_investment_return_2y, last504Chunks.length - 1);
            const irm_bovespa_sharpe_2y = calcSharpeForPeriod(irm_risk_2y, irm_investment_return_2y, irm_bovespa_investment_return_2y, last504Chunks.length - 1);
            const irm_ipca_sharpe_2y = calcSharpeForPeriod(irm_risk_2y, irm_investment_return_2y, irm_ipca_investment_return_2y, last504Chunks.length - 1);
            const irm_igpm_sharpe_2y = calcSharpeForPeriod(irm_risk_2y, irm_investment_return_2y, irm_igpm_investment_return_2y, last504Chunks.length - 1);
            const irm_igpdi_sharpe_2y = calcSharpeForPeriod(irm_risk_2y, irm_investment_return_2y, irm_igpdi_investment_return_2y, last504Chunks.length - 1);
            const irm_dolar_sharpe_2y = calcSharpeForPeriod(irm_risk_2y, irm_investment_return_2y, irm_dolar_investment_return_2y, last504Chunks.length - 1);
            const irm_euro_sharpe_2y = calcSharpeForPeriod(irm_risk_2y, irm_investment_return_2y, irm_euro_investment_return_2y, last504Chunks.length - 1);

            // Sharpe 3Y
            const irm_cdi_sharpe_3y = calcSharpeForPeriod(irm_risk_3y, irm_investment_return_3y, irm_cdi_investment_return_3y, last756Chunks.length - 1);
            const irm_bovespa_sharpe_3y = calcSharpeForPeriod(irm_risk_3y, irm_investment_return_3y, irm_bovespa_investment_return_3y, last756Chunks.length - 1);
            const irm_ipca_sharpe_3y = calcSharpeForPeriod(irm_risk_3y, irm_investment_return_3y, irm_ipca_investment_return_3y, last756Chunks.length - 1);
            const irm_igpm_sharpe_3y = calcSharpeForPeriod(irm_risk_3y, irm_investment_return_3y, irm_igpm_investment_return_3y, last756Chunks.length - 1);
            const irm_igpdi_sharpe_3y = calcSharpeForPeriod(irm_risk_3y, irm_investment_return_3y, irm_igpdi_investment_return_3y, last756Chunks.length - 1);
            const irm_dolar_sharpe_3y = calcSharpeForPeriod(irm_risk_3y, irm_investment_return_3y, irm_dolar_investment_return_3y, last756Chunks.length - 1);
            const irm_euro_sharpe_3y = calcSharpeForPeriod(irm_risk_3y, irm_investment_return_3y, irm_euro_investment_return_3y, last756Chunks.length - 1);

            // Accumulated Sharpe            
            const irm_cdi_accumulated_sharpe = calcSharpeForPeriod(irm_accumulated_risk, irm_accumulated_investment_return, irm_cdi_accumulated_investment_return, chunks.length - 1);
            const irm_bovespa_accumulated_sharpe = calcSharpeForPeriod(irm_accumulated_risk, irm_accumulated_investment_return, irm_bovespa_accumulated_investment_return, chunks.length - 1);
            const irm_ipca_accumulated_sharpe = calcSharpeForPeriod(irm_accumulated_risk, irm_accumulated_investment_return, irm_ipca_accumulated_investment_return, chunks.length - 1);
            const irm_igpm_accumulated_sharpe = calcSharpeForPeriod(irm_accumulated_risk, irm_accumulated_investment_return, irm_igpm_accumulated_investment_return, chunks.length - 1);
            const irm_igpdi_accumulated_sharpe = calcSharpeForPeriod(irm_accumulated_risk, irm_accumulated_investment_return, irm_igpdi_accumulated_investment_return, chunks.length - 1);
            const irm_dolar_accumulated_sharpe = calcSharpeForPeriod(irm_accumulated_risk, irm_accumulated_investment_return, irm_dolar_accumulated_investment_return, chunks.length - 1);
            const irm_euro_accumulated_sharpe = calcSharpeForPeriod(irm_accumulated_risk, irm_accumulated_investment_return, irm_euro_accumulated_investment_return, chunks.length - 1);

            // Consistency MTD        
            const irm_cdi_consistency_mtd = getConsistencyForPeriod(cdiConsistencyReachedMTD[month], lastMTDCDIConsistency[month]);
            const irm_bovespa_consistency_mtd = getConsistencyForPeriod(bovespaConsistencyReachedMTD[month], lastMTDBovespaConsistency[month]);
            const irm_ipca_consistency_mtd = getConsistencyForPeriod(ipcaConsistencyReachedMTD[month], lastMTDIPCAConsistency[month]);
            const irm_igpm_consistency_mtd = getConsistencyForPeriod(igpmConsistencyReachedMTD[month], lastMTDIGPMConsistency[month]);
            const irm_igpdi_consistency_mtd = getConsistencyForPeriod(igpdiConsistencyReachedMTD[month], lastMTDIGPDIConsistency[month]);
            const irm_dolar_consistency_mtd = getConsistencyForPeriod(dolarConsistencyReachedMTD[month], lastMTDDolarConsistency[month]);
            const irm_euro_consistency_mtd = getConsistencyForPeriod(euroConsistencyReachedMTD[month], lastMTDEuroConsistency[month]);

            // Consistency YTD
            const irm_cdi_consistency_ytd = getConsistencyForPeriod(cdiConsistencyReachedYTD[year], lastYTDCDIConsistency[year]);
            const irm_bovespa_consistency_ytd = getConsistencyForPeriod(bovespaConsistencyReachedYTD[year], lastYTDBovespaConsistency[year]);
            const irm_ipca_consistency_ytd = getConsistencyForPeriod(ipcaConsistencyReachedYTD[year], lastYTDIPCAConsistency[year]);
            const irm_igpm_consistency_ytd = getConsistencyForPeriod(igpmConsistencyReachedYTD[year], lastYTDIGPMConsistency[year]);
            const irm_igpdi_consistency_ytd = getConsistencyForPeriod(igpdiConsistencyReachedYTD[year], lastYTDIGPDIConsistency[year]);
            const irm_dolar_consistency_ytd = getConsistencyForPeriod(dolarConsistencyReachedYTD[year], lastYTDDolarConsistency[year]);
            const irm_euro_consistency_ytd = getConsistencyForPeriod(euroConsistencyReachedYTD[year], lastYTDEuroConsistency[year]);

            // Consistency 1M        
            const irm_cdi_consistency_1m = getConsistencyForPeriod(cdiConsistencyReachedLast21, last21CDIConsistency);
            const irm_bovespa_consistency_1m = getConsistencyForPeriod(bovespaConsistencyReachedLast21, last21BovespaConsistency);
            const irm_ipca_consistency_1m = getConsistencyForPeriod(ipcaConsistencyReachedLast21, last21IPCAConsistency);
            const irm_igpm_consistency_1m = getConsistencyForPeriod(igpmConsistencyReachedLast21, last21IGPMConsistency);
            const irm_igpdi_consistency_1m = getConsistencyForPeriod(igpdiConsistencyReachedLast21, last21IGPDIConsistency);
            const irm_dolar_consistency_1m = getConsistencyForPeriod(dolarConsistencyReachedLast21, last21DolarConsistency);
            const irm_euro_consistency_1m = getConsistencyForPeriod(euroConsistencyReachedLast21, last21EuroConsistency);

            // Consistency 3M        
            const irm_cdi_consistency_3m = getConsistencyForPeriod(cdiConsistencyReachedLast63, last63CDIConsistency);
            const irm_bovespa_consistency_3m = getConsistencyForPeriod(bovespaConsistencyReachedLast63, last63BovespaConsistency);
            const irm_ipca_consistency_3m = getConsistencyForPeriod(ipcaConsistencyReachedLast63, last63IPCAConsistency);
            const irm_igpm_consistency_3m = getConsistencyForPeriod(igpmConsistencyReachedLast63, last63IGPMConsistency);
            const irm_igpdi_consistency_3m = getConsistencyForPeriod(igpdiConsistencyReachedLast63, last63IGPDIConsistency);
            const irm_dolar_consistency_3m = getConsistencyForPeriod(dolarConsistencyReachedLast63, last63DolarConsistency);
            const irm_euro_consistency_3m = getConsistencyForPeriod(euroConsistencyReachedLast63, last63EuroConsistency);

            // Consistency 6M        
            const irm_cdi_consistency_6m = getConsistencyForPeriod(cdiConsistencyReachedLast126, last126CDIConsistency);
            const irm_bovespa_consistency_6m = getConsistencyForPeriod(bovespaConsistencyReachedLast126, last126BovespaConsistency);
            const irm_ipca_consistency_6m = getConsistencyForPeriod(ipcaConsistencyReachedLast126, last126IPCAConsistency);
            const irm_igpm_consistency_6m = getConsistencyForPeriod(igpmConsistencyReachedLast126, last126IGPMConsistency);
            const irm_igpdi_consistency_6m = getConsistencyForPeriod(igpdiConsistencyReachedLast126, last126IGPDIConsistency);
            const irm_dolar_consistency_6m = getConsistencyForPeriod(dolarConsistencyReachedLast126, last126DolarConsistency);
            const irm_euro_consistency_6m = getConsistencyForPeriod(euroConsistencyReachedLast126, last126EuroConsistency);

            // Consistency 1Y        
            const irm_cdi_consistency_1y = getConsistencyForPeriod(cdiConsistencyReachedLast252, last252CDIConsistency);
            const irm_bovespa_consistency_1y = getConsistencyForPeriod(bovespaConsistencyReachedLast252, last252BovespaConsistency);
            const irm_ipca_consistency_1y = getConsistencyForPeriod(ipcaConsistencyReachedLast252, last252IPCAConsistency);
            const irm_igpm_consistency_1y = getConsistencyForPeriod(igpmConsistencyReachedLast252, last252IGPMConsistency);
            const irm_igpdi_consistency_1y = getConsistencyForPeriod(igpdiConsistencyReachedLast252, last252IGPDIConsistency);
            const irm_dolar_consistency_1y = getConsistencyForPeriod(dolarConsistencyReachedLast252, last252DolarConsistency);
            const irm_euro_consistency_1y = getConsistencyForPeriod(euroConsistencyReachedLast252, last252EuroConsistency);

            // Consistency 2Y
            const irm_cdi_consistency_2y = getConsistencyForPeriod(cdiConsistencyReachedLast504, last504CDIConsistency);
            const irm_bovespa_consistency_2y = getConsistencyForPeriod(bovespaConsistencyReachedLast504, last504BovespaConsistency);
            const irm_ipca_consistency_2y = getConsistencyForPeriod(ipcaConsistencyReachedLast504, last504IPCAConsistency);
            const irm_igpm_consistency_2y = getConsistencyForPeriod(igpmConsistencyReachedLast504, last504IGPMConsistency);
            const irm_igpdi_consistency_2y = getConsistencyForPeriod(igpdiConsistencyReachedLast504, last504IGPDIConsistency);
            const irm_dolar_consistency_2y = getConsistencyForPeriod(dolarConsistencyReachedLast504, last504DolarConsistency);
            const irm_euro_consistency_2y = getConsistencyForPeriod(euroConsistencyReachedLast504, last504EuroConsistency);

            // Consistency 3Y
            const irm_cdi_consistency_3y = getConsistencyForPeriod(cdiConsistencyReachedLast756, last756CDIConsistency);
            const irm_bovespa_consistency_3y = getConsistencyForPeriod(bovespaConsistencyReachedLast756, last756BovespaConsistency);
            const irm_ipca_consistency_3y = getConsistencyForPeriod(ipcaConsistencyReachedLast756, last756IPCAConsistency);
            const irm_igpm_consistency_3y = getConsistencyForPeriod(igpmConsistencyReachedLast756, last756IGPMConsistency);
            const irm_igpdi_consistency_3y = getConsistencyForPeriod(igpdiConsistencyReachedLast756, last756IGPDIConsistency);
            const irm_dolar_consistency_3y = getConsistencyForPeriod(dolarConsistencyReachedLast756, last756DolarConsistency);
            const irm_euro_consistency_3y = getConsistencyForPeriod(euroConsistencyReachedLast756, last756EuroConsistency);

            // Networth
            const irm_networth = returnFromQuota(lastMonthOrFirstDay(chunk, year, month, lastYear, lastMonth).vl_patrim_liq, chunk.vl_patrim_liq);
            const irm_networth_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].vl_patrim_liq, chunk.vl_patrim_liq);
            const irm_networth_ytd = returnFromQuota(chunksByYear[year][0].vl_patrim_liq, chunk.vl_patrim_liq);
            const irm_networth_1m = returnFromQuota(last21Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            const irm_networth_3m = returnFromQuota(last63Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            const irm_networth_6m = returnFromQuota(last126Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            const irm_networth_1y = returnFromQuota(last252Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            const irm_networth_2y = returnFromQuota(last504Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            const irm_networth_3y = returnFromQuota(last756Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            const irm_accumulated_networth = chunk.vl_patrim_liq;

            // Quotaholders
            const irm_quotaholders = returnFromQuota(lastMonthOrFirstDay(chunk, year, month, lastYear, lastMonth).nr_cotst, chunk.nr_cotst);
            const irm_quotaholders_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].nr_cotst, chunk.nr_cotst);
            const irm_quotaholders_ytd = returnFromQuota(chunksByYear[year][0].nr_cotst, chunk.nr_cotst);
            const irm_quotaholders_1m = returnFromQuota(last21Chunks[0].nr_cotst, chunk.nr_cotst);
            const irm_quotaholders_3m = returnFromQuota(last63Chunks[0].nr_cotst, chunk.nr_cotst);
            const irm_quotaholders_6m = returnFromQuota(last126Chunks[0].nr_cotst, chunk.nr_cotst);
            const irm_quotaholders_1y = returnFromQuota(last252Chunks[0].nr_cotst, chunk.nr_cotst);
            const irm_quotaholders_2y = returnFromQuota(last504Chunks[0].nr_cotst, chunk.nr_cotst);
            const irm_quotaholders_3y = returnFromQuota(last756Chunks[0].nr_cotst, chunk.nr_cotst);
            const irm_accumulated_quotaholders = chunk.nr_cotst;

            const enfOfMonth = chunk.dt_comptc.endOf('month');

            const irm_dt_comptc = `${enfOfMonth.$y}-${string.pad(enfOfMonth.$M + 1, 2)}-${string.pad(enfOfMonth.$D, 2)}`;

            if (startSavingFrom == null || irm_dt_comptc >= startSavingFrom) {
                stream.push({
                    table: 'investment_return_monthly',
                    primaryKey: ['irm_CNPJ_FUNDO', 'irm_DT_COMPTC'],
                    fields: {
                        irm_id: uuidv1(),
                        irm_cnpj_fundo: chunk.cnpj_fundo,
                        irm_dt_comptc,
                        irm_investment_return,
                        irm_cdi_investment_return,
                        irm_bovespa_investment_return,
                        irm_ipca_investment_return,
                        irm_igpm_investment_return,
                        irm_igpdi_investment_return,
                        irm_dolar_investment_return,
                        irm_euro_investment_return,
                        irm_investment_return_mtd,
                        irm_cdi_investment_return_mtd,
                        irm_bovespa_investment_return_mtd,
                        irm_ipca_investment_return_mtd,
                        irm_igpm_investment_return_mtd,
                        irm_igpdi_investment_return_mtd,
                        irm_dolar_investment_return_mtd,
                        irm_euro_investment_return_mtd,
                        irm_investment_return_ytd,
                        irm_cdi_investment_return_ytd,
                        irm_bovespa_investment_return_ytd,
                        irm_ipca_investment_return_ytd,
                        irm_igpm_investment_return_ytd,
                        irm_igpdi_investment_return_ytd,
                        irm_dolar_investment_return_ytd,
                        irm_euro_investment_return_ytd,
                        irm_investment_return_1m,
                        irm_cdi_investment_return_1m,
                        irm_bovespa_investment_return_1m,
                        irm_ipca_investment_return_1m,
                        irm_igpm_investment_return_1m,
                        irm_igpdi_investment_return_1m,
                        irm_dolar_investment_return_1m,
                        irm_euro_investment_return_1m,
                        irm_investment_return_3m,
                        irm_cdi_investment_return_3m,
                        irm_bovespa_investment_return_3m,
                        irm_ipca_investment_return_3m,
                        irm_igpm_investment_return_3m,
                        irm_igpdi_investment_return_3m,
                        irm_dolar_investment_return_3m,
                        irm_euro_investment_return_3m,
                        irm_investment_return_6m,
                        irm_cdi_investment_return_6m,
                        irm_bovespa_investment_return_6m,
                        irm_ipca_investment_return_6m,
                        irm_igpm_investment_return_6m,
                        irm_igpdi_investment_return_6m,
                        irm_dolar_investment_return_6m,
                        irm_euro_investment_return_6m,
                        irm_investment_return_1y,
                        irm_cdi_investment_return_1y,
                        irm_bovespa_investment_return_1y,
                        irm_ipca_investment_return_1y,
                        irm_igpm_investment_return_1y,
                        irm_igpdi_investment_return_1y,
                        irm_dolar_investment_return_1y,
                        irm_euro_investment_return_1y,
                        irm_investment_return_2y,
                        irm_cdi_investment_return_2y,
                        irm_bovespa_investment_return_2y,
                        irm_ipca_investment_return_2y,
                        irm_igpm_investment_return_2y,
                        irm_igpdi_investment_return_2y,
                        irm_dolar_investment_return_2y,
                        irm_euro_investment_return_2y,
                        irm_investment_return_3y,
                        irm_cdi_investment_return_3y,
                        irm_bovespa_investment_return_3y,
                        irm_ipca_investment_return_3y,
                        irm_igpm_investment_return_3y,
                        irm_igpdi_investment_return_3y,
                        irm_dolar_investment_return_3y,
                        irm_euro_investment_return_3y,
                        irm_accumulated_investment_return,
                        irm_cdi_accumulated_investment_return,
                        irm_bovespa_accumulated_investment_return,
                        irm_ipca_accumulated_investment_return,
                        irm_igpm_accumulated_investment_return,
                        irm_igpdi_accumulated_investment_return,
                        irm_dolar_accumulated_investment_return,
                        irm_euro_accumulated_investment_return,
                        irm_risk,
                        irm_risk_mtd,
                        irm_risk_ytd,
                        irm_risk_1m,
                        irm_risk_3m,
                        irm_risk_6m,
                        irm_risk_1y,
                        irm_risk_2y,
                        irm_risk_3y,
                        irm_accumulated_risk,
                        irm_cdi_sharpe,
                        irm_bovespa_sharpe,
                        irm_ipca_sharpe,
                        irm_igpm_sharpe,
                        irm_igpdi_sharpe,
                        irm_dolar_sharpe,
                        irm_euro_sharpe,
                        irm_cdi_sharpe_mtd,
                        irm_bovespa_sharpe_mtd,
                        irm_ipca_sharpe_mtd,
                        irm_igpm_sharpe_mtd,
                        irm_igpdi_sharpe_mtd,
                        irm_dolar_sharpe_mtd,
                        irm_euro_sharpe_mtd,
                        irm_cdi_sharpe_ytd,
                        irm_bovespa_sharpe_ytd,
                        irm_ipca_sharpe_ytd,
                        irm_igpm_sharpe_ytd,
                        irm_igpdi_sharpe_ytd,
                        irm_dolar_sharpe_ytd,
                        irm_euro_sharpe_ytd,
                        irm_cdi_sharpe_1m,
                        irm_bovespa_sharpe_1m,
                        irm_ipca_sharpe_1m,
                        irm_igpm_sharpe_1m,
                        irm_igpdi_sharpe_1m,
                        irm_dolar_sharpe_1m,
                        irm_euro_sharpe_1m,
                        irm_cdi_sharpe_3m,
                        irm_bovespa_sharpe_3m,
                        irm_ipca_sharpe_3m,
                        irm_igpm_sharpe_3m,
                        irm_igpdi_sharpe_3m,
                        irm_dolar_sharpe_3m,
                        irm_euro_sharpe_3m,
                        irm_cdi_sharpe_6m,
                        irm_bovespa_sharpe_6m,
                        irm_ipca_sharpe_6m,
                        irm_igpm_sharpe_6m,
                        irm_igpdi_sharpe_6m,
                        irm_dolar_sharpe_6m,
                        irm_euro_sharpe_6m,
                        irm_cdi_sharpe_1y,
                        irm_bovespa_sharpe_1y,
                        irm_ipca_sharpe_1y,
                        irm_igpm_sharpe_1y,
                        irm_igpdi_sharpe_1y,
                        irm_dolar_sharpe_1y,
                        irm_euro_sharpe_1y,
                        irm_cdi_sharpe_2y,
                        irm_bovespa_sharpe_2y,
                        irm_ipca_sharpe_2y,
                        irm_igpm_sharpe_2y,
                        irm_igpdi_sharpe_2y,
                        irm_dolar_sharpe_2y,
                        irm_euro_sharpe_2y,
                        irm_cdi_sharpe_3y,
                        irm_bovespa_sharpe_3y,
                        irm_ipca_sharpe_3y,
                        irm_igpm_sharpe_3y,
                        irm_igpdi_sharpe_3y,
                        irm_dolar_sharpe_3y,
                        irm_euro_sharpe_3y,
                        irm_cdi_accumulated_sharpe,
                        irm_bovespa_accumulated_sharpe,
                        irm_ipca_accumulated_sharpe,
                        irm_igpm_accumulated_sharpe,
                        irm_igpdi_accumulated_sharpe,
                        irm_dolar_accumulated_sharpe,
                        irm_euro_accumulated_sharpe,
                        irm_cdi_consistency_mtd,
                        irm_bovespa_consistency_mtd,
                        irm_ipca_consistency_mtd,
                        irm_igpm_consistency_mtd,
                        irm_igpdi_consistency_mtd,
                        irm_dolar_consistency_mtd,
                        irm_euro_consistency_mtd,
                        irm_cdi_consistency_ytd,
                        irm_bovespa_consistency_ytd,
                        irm_ipca_consistency_ytd,
                        irm_igpm_consistency_ytd,
                        irm_igpdi_consistency_ytd,
                        irm_dolar_consistency_ytd,
                        irm_euro_consistency_ytd,
                        irm_cdi_consistency_1m,
                        irm_bovespa_consistency_1m,
                        irm_ipca_consistency_1m,
                        irm_igpm_consistency_1m,
                        irm_igpdi_consistency_1m,
                        irm_dolar_consistency_1m,
                        irm_euro_consistency_1m,
                        irm_cdi_consistency_3m,
                        irm_bovespa_consistency_3m,
                        irm_ipca_consistency_3m,
                        irm_igpm_consistency_3m,
                        irm_igpdi_consistency_3m,
                        irm_dolar_consistency_3m,
                        irm_euro_consistency_3m,
                        irm_cdi_consistency_6m,
                        irm_bovespa_consistency_6m,
                        irm_ipca_consistency_6m,
                        irm_igpm_consistency_6m,
                        irm_igpdi_consistency_6m,
                        irm_dolar_consistency_6m,
                        irm_euro_consistency_6m,
                        irm_cdi_consistency_1y,
                        irm_bovespa_consistency_1y,
                        irm_ipca_consistency_1y,
                        irm_igpm_consistency_1y,
                        irm_igpdi_consistency_1y,
                        irm_dolar_consistency_1y,
                        irm_euro_consistency_1y,
                        irm_cdi_consistency_2y,
                        irm_bovespa_consistency_2y,
                        irm_ipca_consistency_2y,
                        irm_igpm_consistency_2y,
                        irm_igpdi_consistency_2y,
                        irm_dolar_consistency_2y,
                        irm_euro_consistency_2y,
                        irm_cdi_consistency_3y,
                        irm_bovespa_consistency_3y,
                        irm_ipca_consistency_3y,
                        irm_igpm_consistency_3y,
                        irm_igpdi_consistency_3y,
                        irm_dolar_consistency_3y,
                        irm_euro_consistency_3y,
                        irm_networth,
                        irm_accumulated_networth,
                        irm_networth_mtd,
                        irm_networth_ytd,
                        irm_networth_1m,
                        irm_networth_3m,
                        irm_networth_6m,
                        irm_networth_1y,
                        irm_networth_2y,
                        irm_networth_3y,
                        irm_quotaholders,
                        irm_accumulated_quotaholders,
                        irm_quotaholders_mtd,
                        irm_quotaholders_ytd,
                        irm_quotaholders_1m,
                        irm_quotaholders_3m,
                        irm_quotaholders_6m,
                        irm_quotaholders_1y,
                        irm_quotaholders_2y,
                        irm_quotaholders_3y
                    }
                });
            }
        }
    };

    const processYearly = (stream, chunk) => {
        const yearBefore = chunk.dt_comptc.subtract(1, 'year');
        const lastYear = getYearHash(yearBefore);
        const month = getMonthHash(chunk.dt_comptc);
        const year = getYearHash(chunk.dt_comptc);

        if (risk) {
            // Return            
            const iry_investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).bovespa_valor, chunk.bovespa_valor);
            const iry_ipca_investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).ipca_quota, chunk.ipca_quota);
            const iry_igpm_investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).igpm_quota, chunk.igpm_quota);
            const iry_igpdi_investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).igpdi_quota, chunk.igpdi_quota);
            const iry_dolar_investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).dolar_valor, chunk.dolar_valor);
            const iry_euro_investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).euro_valor, chunk.euro_valor);

            // Return MTD
            const iry_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].bovespa_valor, chunk.bovespa_valor);
            const iry_ipca_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].ipca_quota, chunk.ipca_quota);
            const iry_igpm_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].igpm_quota, chunk.igpm_quota);
            const iry_igpdi_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].igpdi_quota, chunk.igpdi_quota);
            const iry_dolar_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].dolar_valor, chunk.dolar_valor);
            const iry_euro_investment_return_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].euro_valor, chunk.euro_valor);

            // Return YTD
            const iry_investment_return_ytd = returnFromQuota(chunksByYear[year][0].vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return_ytd = returnFromQuota(chunksByYear[year][0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return_ytd = returnFromQuota(chunksByYear[year][0].bovespa_valor, chunk.bovespa_valor);
            const iry_ipca_investment_return_ytd = returnFromQuota(chunksByYear[year][0].ipca_quota, chunk.ipca_quota);
            const iry_igpm_investment_return_ytd = returnFromQuota(chunksByYear[year][0].igpm_quota, chunk.igpm_quota);
            const iry_igpdi_investment_return_ytd = returnFromQuota(chunksByYear[year][0].igpdi_quota, chunk.igpdi_quota);
            const iry_dolar_investment_return_ytd = returnFromQuota(chunksByYear[year][0].dolar_valor, chunk.dolar_valor);
            const iry_euro_investment_return_ytd = returnFromQuota(chunksByYear[year][0].euro_valor, chunk.euro_valor);

            // Return 1M
            const iry_investment_return_1m = returnFromQuota(last21Chunks[0].vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return_1m = returnFromQuota(last21Chunks[0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return_1m = returnFromQuota(last21Chunks[0].bovespa_valor, chunk.bovespa_valor);
            const iry_ipca_investment_return_1m = returnFromQuota(last21Chunks[0].ipca_quota, chunk.ipca_quota);
            const iry_igpm_investment_return_1m = returnFromQuota(last21Chunks[0].igpm_quota, chunk.igpm_quota);
            const iry_igpdi_investment_return_1m = returnFromQuota(last21Chunks[0].igpdi_quota, chunk.igpdi_quota);
            const iry_dolar_investment_return_1m = returnFromQuota(last21Chunks[0].dolar_valor, chunk.dolar_valor);
            const iry_euro_investment_return_1m = returnFromQuota(last21Chunks[0].euro_valor, chunk.euro_valor);

            // Return 3M
            const iry_investment_return_3m = returnFromQuota(last63Chunks[0].vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return_3m = returnFromQuota(last63Chunks[0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return_3m = returnFromQuota(last63Chunks[0].bovespa_valor, chunk.bovespa_valor);
            const iry_ipca_investment_return_3m = returnFromQuota(last63Chunks[0].ipca_quota, chunk.ipca_quota);
            const iry_igpm_investment_return_3m = returnFromQuota(last63Chunks[0].igpm_quota, chunk.igpm_quota);
            const iry_igpdi_investment_return_3m = returnFromQuota(last63Chunks[0].igpdi_quota, chunk.igpdi_quota);
            const iry_dolar_investment_return_3m = returnFromQuota(last63Chunks[0].dolar_valor, chunk.dolar_valor);
            const iry_euro_investment_return_3m = returnFromQuota(last63Chunks[0].euro_valor, chunk.euro_valor);

            // Return 6M
            const iry_investment_return_6m = returnFromQuota(last126Chunks[0].vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return_6m = returnFromQuota(last126Chunks[0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return_6m = returnFromQuota(last126Chunks[0].bovespa_valor, chunk.bovespa_valor);
            const iry_ipca_investment_return_6m = returnFromQuota(last126Chunks[0].ipca_quota, chunk.ipca_quota);
            const iry_igpm_investment_return_6m = returnFromQuota(last126Chunks[0].igpm_quota, chunk.igpm_quota);
            const iry_igpdi_investment_return_6m = returnFromQuota(last126Chunks[0].igpdi_quota, chunk.igpdi_quota);
            const iry_dolar_investment_return_6m = returnFromQuota(last126Chunks[0].dolar_valor, chunk.dolar_valor);
            const iry_euro_investment_return_6m = returnFromQuota(last126Chunks[0].euro_valor, chunk.euro_valor);

            // Return 1Y            
            const iry_investment_return_1y = returnFromQuota(last252Chunks[0].vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return_1y = returnFromQuota(last252Chunks[0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return_1y = returnFromQuota(last252Chunks[0].bovespa_valor, chunk.bovespa_valor);
            const iry_ipca_investment_return_1y = returnFromQuota(last252Chunks[0].ipca_quota, chunk.ipca_quota);
            const iry_igpm_investment_return_1y = returnFromQuota(last252Chunks[0].igpm_quota, chunk.igpm_quota);
            const iry_igpdi_investment_return_1y = returnFromQuota(last252Chunks[0].igpdi_quota, chunk.igpdi_quota);
            const iry_dolar_investment_return_1y = returnFromQuota(last252Chunks[0].dolar_valor, chunk.dolar_valor);
            const iry_euro_investment_return_1y = returnFromQuota(last252Chunks[0].euro_valor, chunk.euro_valor);

            // Return 2Y
            const iry_investment_return_2y = returnFromQuota(last504Chunks[0].vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return_2y = returnFromQuota(last504Chunks[0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return_2y = returnFromQuota(last504Chunks[0].bovespa_valor, chunk.bovespa_valor);
            const iry_ipca_investment_return_2y = returnFromQuota(last504Chunks[0].ipca_quota, chunk.ipca_quota);
            const iry_igpm_investment_return_2y = returnFromQuota(last504Chunks[0].igpm_quota, chunk.igpm_quota);
            const iry_igpdi_investment_return_2y = returnFromQuota(last504Chunks[0].igpdi_quota, chunk.igpdi_quota);
            const iry_dolar_investment_return_2y = returnFromQuota(last504Chunks[0].dolar_valor, chunk.dolar_valor);
            const iry_euro_investment_return_2y = returnFromQuota(last504Chunks[0].euro_valor, chunk.euro_valor);

            // Return 3Y
            const iry_investment_return_3y = returnFromQuota(last756Chunks[0].vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return_3y = returnFromQuota(last756Chunks[0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return_3y = returnFromQuota(last756Chunks[0].bovespa_valor, chunk.bovespa_valor);
            const iry_ipca_investment_return_3y = returnFromQuota(last756Chunks[0].ipca_quota, chunk.ipca_quota);
            const iry_igpm_investment_return_3y = returnFromQuota(last756Chunks[0].igpm_quota, chunk.igpm_quota);
            const iry_igpdi_investment_return_3y = returnFromQuota(last756Chunks[0].igpdi_quota, chunk.igpdi_quota);
            const iry_dolar_investment_return_3y = returnFromQuota(last756Chunks[0].dolar_valor, chunk.dolar_valor);
            const iry_euro_investment_return_3y = returnFromQuota(last756Chunks[0].euro_valor, chunk.euro_valor);

            // Accumulated Return            
            const iry_accumulated_investment_return = returnFromQuota(chunks[0].vl_quota, chunk.vl_quota);
            const iry_cdi_accumulated_investment_return = returnFromQuota(chunks[0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_accumulated_investment_return = returnFromQuota(chunks[0].bovespa_valor, chunk.bovespa_valor);
            const iry_ipca_accumulated_investment_return = returnFromQuota(chunks[0].ipca_quota, chunk.ipca_quota);
            const iry_igpm_accumulated_investment_return = returnFromQuota(chunks[0].igpm_quota, chunk.igpm_quota);
            const iry_igpdi_accumulated_investment_return = returnFromQuota(chunks[0].igpdi_quota, chunk.igpdi_quota);
            const iry_dolar_accumulated_investment_return = returnFromQuota(chunks[0].dolar_valor, chunk.dolar_valor);
            const iry_euro_accumulated_investment_return = returnFromQuota(chunks[0].euro_valor, chunk.euro_valor);

            // Risk            
            const iry_risk = risksByYear[year].get() * Math.sqrt(252);

            // Risk MTD
            const iry_risk_mtd = risksByMonth[month].get() * Math.sqrt(252);

            // Risk YTD
            const iry_risk_ytd = risksByYear[year].get() * Math.sqrt(252);

            // Risk 1M            
            const iry_risk_1m = last21Risk.get() * Math.sqrt(252);

            // Risk 3M            
            const iry_risk_3m = last63Risk.get() * Math.sqrt(252);

            // Risk 6M            
            const iry_risk_6m = last126Risk.get() * Math.sqrt(252);

            // Risk 1Y            
            const iry_risk_1y = last252Risk.get() * Math.sqrt(252);

            // Risk 2Y            
            const iry_risk_2y = last504Risk.get() * Math.sqrt(252);

            // Risk 3Y            
            const iry_risk_3y = last756Risk.get() * Math.sqrt(252);

            // Accumulated Risk
            const iry_accumulated_risk = risk.get() * Math.sqrt(252);

            // Sharpe
            const iry_cdi_sharpe = calcSharpeForPeriod(iry_risk, iry_investment_return, iry_cdi_investment_return, Object.values(chunksByYearAndMonth[year]).reduce((acc, month) => acc + month.length - 1, 0));
            const iry_bovespa_sharpe = calcSharpeForPeriod(iry_risk, iry_investment_return, iry_bovespa_investment_return, Object.values(chunksByYearAndMonth[year]).reduce((acc, month) => acc + month.length - 1, 0));
            const iry_ipca_sharpe = calcSharpeForPeriod(iry_risk, iry_investment_return, iry_ipca_investment_return, Object.values(chunksByYearAndMonth[year]).reduce((acc, month) => acc + month.length - 1, 0));
            const iry_igpm_sharpe = calcSharpeForPeriod(iry_risk, iry_investment_return, iry_igpm_investment_return, Object.values(chunksByYearAndMonth[year]).reduce((acc, month) => acc + month.length - 1, 0));
            const iry_igpdi_sharpe = calcSharpeForPeriod(iry_risk, iry_investment_return, iry_igpdi_investment_return, Object.values(chunksByYearAndMonth[year]).reduce((acc, month) => acc + month.length - 1, 0));
            const iry_dolar_sharpe = calcSharpeForPeriod(iry_risk, iry_investment_return, iry_dolar_investment_return, Object.values(chunksByYearAndMonth[year]).reduce((acc, month) => acc + month.length - 1, 0));
            const iry_euro_sharpe = calcSharpeForPeriod(iry_risk, iry_investment_return, iry_euro_investment_return, Object.values(chunksByYearAndMonth[year]).reduce((acc, month) => acc + month.length - 1, 0));

            // Sharpe MTD
            const iry_cdi_sharpe_mtd = calcSharpeForPeriod(iry_risk_mtd, iry_investment_return_mtd, iry_cdi_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            const iry_bovespa_sharpe_mtd = calcSharpeForPeriod(iry_risk_mtd, iry_investment_return_mtd, iry_bovespa_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            const iry_ipca_sharpe_mtd = calcSharpeForPeriod(iry_risk_mtd, iry_investment_return_mtd, iry_ipca_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            const iry_igpm_sharpe_mtd = calcSharpeForPeriod(iry_risk_mtd, iry_investment_return_mtd, iry_igpm_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            const iry_igpdi_sharpe_mtd = calcSharpeForPeriod(iry_risk_mtd, iry_investment_return_mtd, iry_igpdi_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            const iry_dolar_sharpe_mtd = calcSharpeForPeriod(iry_risk_mtd, iry_investment_return_mtd, iry_dolar_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);
            const iry_euro_sharpe_mtd = calcSharpeForPeriod(iry_risk_mtd, iry_investment_return_mtd, iry_euro_investment_return_mtd, chunksByYearAndMonth[year][month].length - 1);

            // Sharpe YTD
            const iry_cdi_sharpe_ytd = calcSharpeForPeriod(iry_risk_ytd, iry_investment_return_ytd, iry_cdi_investment_return_ytd, chunksByYear[year].length - 1);
            const iry_bovespa_sharpe_ytd = calcSharpeForPeriod(iry_risk_ytd, iry_investment_return_ytd, iry_bovespa_investment_return_ytd, chunksByYear[year].length - 1);
            const iry_ipca_sharpe_ytd = calcSharpeForPeriod(iry_risk_ytd, iry_investment_return_ytd, iry_ipca_investment_return_ytd, chunksByYear[year].length - 1);
            const iry_igpm_sharpe_ytd = calcSharpeForPeriod(iry_risk_ytd, iry_investment_return_ytd, iry_igpm_investment_return_ytd, chunksByYear[year].length - 1);
            const iry_igpdi_sharpe_ytd = calcSharpeForPeriod(iry_risk_ytd, iry_investment_return_ytd, iry_igpdi_investment_return_ytd, chunksByYear[year].length - 1);
            const iry_dolar_sharpe_ytd = calcSharpeForPeriod(iry_risk_ytd, iry_investment_return_ytd, iry_dolar_investment_return_ytd, chunksByYear[year].length - 1);
            const iry_euro_sharpe_ytd = calcSharpeForPeriod(iry_risk_ytd, iry_investment_return_ytd, iry_euro_investment_return_ytd, chunksByYear[year].length - 1);

            // Sharpe 1M
            const iry_cdi_sharpe_1m = calcSharpeForPeriod(iry_risk_1m, iry_investment_return_1m, iry_cdi_investment_return_1m, last21Chunks.length - 1);
            const iry_bovespa_sharpe_1m = calcSharpeForPeriod(iry_risk_1m, iry_investment_return_1m, iry_bovespa_investment_return_1m, last21Chunks.length - 1);
            const iry_ipca_sharpe_1m = calcSharpeForPeriod(iry_risk_1m, iry_investment_return_1m, iry_ipca_investment_return_1m, last21Chunks.length - 1);
            const iry_igpm_sharpe_1m = calcSharpeForPeriod(iry_risk_1m, iry_investment_return_1m, iry_igpm_investment_return_1m, last21Chunks.length - 1);
            const iry_igpdi_sharpe_1m = calcSharpeForPeriod(iry_risk_1m, iry_investment_return_1m, iry_igpdi_investment_return_1m, last21Chunks.length - 1);
            const iry_dolar_sharpe_1m = calcSharpeForPeriod(iry_risk_1m, iry_investment_return_1m, iry_dolar_investment_return_1m, last21Chunks.length - 1);
            const iry_euro_sharpe_1m = calcSharpeForPeriod(iry_risk_1m, iry_investment_return_1m, iry_euro_investment_return_1m, last21Chunks.length - 1);

            // Sharpe 3M
            const iry_cdi_sharpe_3m = calcSharpeForPeriod(iry_risk_3m, iry_investment_return_3m, iry_cdi_investment_return_3m, last63Chunks.length - 1);
            const iry_bovespa_sharpe_3m = calcSharpeForPeriod(iry_risk_3m, iry_investment_return_3m, iry_bovespa_investment_return_3m, last63Chunks.length - 1);
            const iry_ipca_sharpe_3m = calcSharpeForPeriod(iry_risk_3m, iry_investment_return_3m, iry_ipca_investment_return_3m, last63Chunks.length - 1);
            const iry_igpm_sharpe_3m = calcSharpeForPeriod(iry_risk_3m, iry_investment_return_3m, iry_igpm_investment_return_3m, last63Chunks.length - 1);
            const iry_igpdi_sharpe_3m = calcSharpeForPeriod(iry_risk_3m, iry_investment_return_3m, iry_igpdi_investment_return_3m, last63Chunks.length - 1);
            const iry_dolar_sharpe_3m = calcSharpeForPeriod(iry_risk_3m, iry_investment_return_3m, iry_dolar_investment_return_3m, last63Chunks.length - 1);
            const iry_euro_sharpe_3m = calcSharpeForPeriod(iry_risk_3m, iry_investment_return_3m, iry_euro_investment_return_3m, last63Chunks.length - 1);

            // Sharpe 6M
            const iry_cdi_sharpe_6m = calcSharpeForPeriod(iry_risk_6m, iry_investment_return_6m, iry_cdi_investment_return_6m, last126Chunks.length - 1);
            const iry_bovespa_sharpe_6m = calcSharpeForPeriod(iry_risk_6m, iry_investment_return_6m, iry_bovespa_investment_return_6m, last126Chunks.length - 1);
            const iry_ipca_sharpe_6m = calcSharpeForPeriod(iry_risk_6m, iry_investment_return_6m, iry_ipca_investment_return_6m, last126Chunks.length - 1);
            const iry_igpm_sharpe_6m = calcSharpeForPeriod(iry_risk_6m, iry_investment_return_6m, iry_igpm_investment_return_6m, last126Chunks.length - 1);
            const iry_igpdi_sharpe_6m = calcSharpeForPeriod(iry_risk_6m, iry_investment_return_6m, iry_igpdi_investment_return_6m, last126Chunks.length - 1);
            const iry_dolar_sharpe_6m = calcSharpeForPeriod(iry_risk_6m, iry_investment_return_6m, iry_dolar_investment_return_6m, last126Chunks.length - 1);
            const iry_euro_sharpe_6m = calcSharpeForPeriod(iry_risk_6m, iry_investment_return_6m, iry_euro_investment_return_6m, last126Chunks.length - 1);

            // Sharpe 1Y
            const iry_cdi_sharpe_1y = calcSharpeForPeriod(iry_risk_1y, iry_investment_return_1y, iry_cdi_investment_return_1y, last252Chunks.length - 1);
            const iry_bovespa_sharpe_1y = calcSharpeForPeriod(iry_risk_1y, iry_investment_return_1y, iry_bovespa_investment_return_1y, last252Chunks.length - 1);
            const iry_ipca_sharpe_1y = calcSharpeForPeriod(iry_risk_1y, iry_investment_return_1y, iry_ipca_investment_return_1y, last252Chunks.length - 1);
            const iry_igpm_sharpe_1y = calcSharpeForPeriod(iry_risk_1y, iry_investment_return_1y, iry_igpm_investment_return_1y, last252Chunks.length - 1);
            const iry_igpdi_sharpe_1y = calcSharpeForPeriod(iry_risk_1y, iry_investment_return_1y, iry_igpdi_investment_return_1y, last252Chunks.length - 1);
            const iry_dolar_sharpe_1y = calcSharpeForPeriod(iry_risk_1y, iry_investment_return_1y, iry_dolar_investment_return_1y, last252Chunks.length - 1);
            const iry_euro_sharpe_1y = calcSharpeForPeriod(iry_risk_1y, iry_investment_return_1y, iry_euro_investment_return_1y, last252Chunks.length - 1);

            // Sharpe 2Y
            const iry_cdi_sharpe_2y = calcSharpeForPeriod(iry_risk_2y, iry_investment_return_2y, iry_cdi_investment_return_2y, last504Chunks.length - 1);
            const iry_bovespa_sharpe_2y = calcSharpeForPeriod(iry_risk_2y, iry_investment_return_2y, iry_bovespa_investment_return_2y, last504Chunks.length - 1);
            const iry_ipca_sharpe_2y = calcSharpeForPeriod(iry_risk_2y, iry_investment_return_2y, iry_ipca_investment_return_2y, last504Chunks.length - 1);
            const iry_igpm_sharpe_2y = calcSharpeForPeriod(iry_risk_2y, iry_investment_return_2y, iry_igpm_investment_return_2y, last504Chunks.length - 1);
            const iry_igpdi_sharpe_2y = calcSharpeForPeriod(iry_risk_2y, iry_investment_return_2y, iry_igpdi_investment_return_2y, last504Chunks.length - 1);
            const iry_dolar_sharpe_2y = calcSharpeForPeriod(iry_risk_2y, iry_investment_return_2y, iry_dolar_investment_return_2y, last504Chunks.length - 1);
            const iry_euro_sharpe_2y = calcSharpeForPeriod(iry_risk_2y, iry_investment_return_2y, iry_euro_investment_return_2y, last504Chunks.length - 1);

            // Sharpe 3Y
            const iry_cdi_sharpe_3y = calcSharpeForPeriod(iry_risk_3y, iry_investment_return_3y, iry_cdi_investment_return_3y, last756Chunks.length - 1);
            const iry_bovespa_sharpe_3y = calcSharpeForPeriod(iry_risk_3y, iry_investment_return_3y, iry_bovespa_investment_return_3y, last756Chunks.length - 1);
            const iry_ipca_sharpe_3y = calcSharpeForPeriod(iry_risk_3y, iry_investment_return_3y, iry_ipca_investment_return_3y, last756Chunks.length - 1);
            const iry_igpm_sharpe_3y = calcSharpeForPeriod(iry_risk_3y, iry_investment_return_3y, iry_igpm_investment_return_3y, last756Chunks.length - 1);
            const iry_igpdi_sharpe_3y = calcSharpeForPeriod(iry_risk_3y, iry_investment_return_3y, iry_igpdi_investment_return_3y, last756Chunks.length - 1);
            const iry_dolar_sharpe_3y = calcSharpeForPeriod(iry_risk_3y, iry_investment_return_3y, iry_dolar_investment_return_3y, last756Chunks.length - 1);
            const iry_euro_sharpe_3y = calcSharpeForPeriod(iry_risk_3y, iry_investment_return_3y, iry_euro_investment_return_3y, last756Chunks.length - 1);

            // Accumulated Sharpe            
            const iry_cdi_accumulated_sharpe = calcSharpeForPeriod(iry_accumulated_risk, iry_accumulated_investment_return, iry_cdi_accumulated_investment_return, chunks.length - 1);
            const iry_bovespa_accumulated_sharpe = calcSharpeForPeriod(iry_accumulated_risk, iry_accumulated_investment_return, iry_bovespa_accumulated_investment_return, chunks.length - 1);
            const iry_ipca_accumulated_sharpe = calcSharpeForPeriod(iry_accumulated_risk, iry_accumulated_investment_return, iry_ipca_accumulated_investment_return, chunks.length - 1);
            const iry_igpm_accumulated_sharpe = calcSharpeForPeriod(iry_accumulated_risk, iry_accumulated_investment_return, iry_igpm_accumulated_investment_return, chunks.length - 1);
            const iry_igpdi_accumulated_sharpe = calcSharpeForPeriod(iry_accumulated_risk, iry_accumulated_investment_return, iry_igpdi_accumulated_investment_return, chunks.length - 1);
            const iry_dolar_accumulated_sharpe = calcSharpeForPeriod(iry_accumulated_risk, iry_accumulated_investment_return, iry_dolar_accumulated_investment_return, chunks.length - 1);
            const iry_euro_accumulated_sharpe = calcSharpeForPeriod(iry_accumulated_risk, iry_accumulated_investment_return, iry_euro_accumulated_investment_return, chunks.length - 1);

            // Consistency MTD        
            const iry_cdi_consistency_mtd = getConsistencyForPeriod(cdiConsistencyReachedMTD[month], lastMTDCDIConsistency[month]);
            const iry_bovespa_consistency_mtd = getConsistencyForPeriod(bovespaConsistencyReachedMTD[month], lastMTDBovespaConsistency[month]);
            const iry_ipca_consistency_mtd = getConsistencyForPeriod(ipcaConsistencyReachedMTD[month], lastMTDIPCAConsistency[month]);
            const iry_igpm_consistency_mtd = getConsistencyForPeriod(igpmConsistencyReachedMTD[month], lastMTDIGPMConsistency[month]);
            const iry_igpdi_consistency_mtd = getConsistencyForPeriod(igpdiConsistencyReachedMTD[month], lastMTDIGPDIConsistency[month]);
            const iry_dolar_consistency_mtd = getConsistencyForPeriod(dolarConsistencyReachedMTD[month], lastMTDDolarConsistency[month]);
            const iry_euro_consistency_mtd = getConsistencyForPeriod(euroConsistencyReachedMTD[month], lastMTDEuroConsistency[month]);

            // Consistency YTD
            const iry_cdi_consistency_ytd = getConsistencyForPeriod(cdiConsistencyReachedYTD[year], lastYTDCDIConsistency[year]);
            const iry_bovespa_consistency_ytd = getConsistencyForPeriod(bovespaConsistencyReachedYTD[year], lastYTDBovespaConsistency[year]);
            const iry_ipca_consistency_ytd = getConsistencyForPeriod(ipcaConsistencyReachedYTD[year], lastYTDIPCAConsistency[year]);
            const iry_igpm_consistency_ytd = getConsistencyForPeriod(igpmConsistencyReachedYTD[year], lastYTDIGPMConsistency[year]);
            const iry_igpdi_consistency_ytd = getConsistencyForPeriod(igpdiConsistencyReachedYTD[year], lastYTDIGPDIConsistency[year]);
            const iry_dolar_consistency_ytd = getConsistencyForPeriod(dolarConsistencyReachedYTD[year], lastYTDDolarConsistency[year]);
            const iry_euro_consistency_ytd = getConsistencyForPeriod(euroConsistencyReachedYTD[year], lastYTDEuroConsistency[year]);

            // Consistency 1M
            const iry_cdi_consistency_1m = getConsistencyForPeriod(cdiConsistencyReachedLast21, last21CDIConsistency);
            const iry_bovespa_consistency_1m = getConsistencyForPeriod(bovespaConsistencyReachedLast21, last21BovespaConsistency);
            const iry_ipca_consistency_1m = getConsistencyForPeriod(ipcaConsistencyReachedLast21, last21IPCAConsistency);
            const iry_igpm_consistency_1m = getConsistencyForPeriod(igpmConsistencyReachedLast21, last21IGPMConsistency);
            const iry_igpdi_consistency_1m = getConsistencyForPeriod(igpdiConsistencyReachedLast21, last21IGPDIConsistency);
            const iry_dolar_consistency_1m = getConsistencyForPeriod(dolarConsistencyReachedLast21, last21DolarConsistency);
            const iry_euro_consistency_1m = getConsistencyForPeriod(euroConsistencyReachedLast21, last21EuroConsistency);

            // Consistency 3M
            const iry_cdi_consistency_3m = getConsistencyForPeriod(cdiConsistencyReachedLast63, last63CDIConsistency);
            const iry_bovespa_consistency_3m = getConsistencyForPeriod(bovespaConsistencyReachedLast63, last63BovespaConsistency);
            const iry_ipca_consistency_3m = getConsistencyForPeriod(ipcaConsistencyReachedLast63, last63IPCAConsistency);
            const iry_igpm_consistency_3m = getConsistencyForPeriod(igpmConsistencyReachedLast63, last63IGPMConsistency);
            const iry_igpdi_consistency_3m = getConsistencyForPeriod(igpdiConsistencyReachedLast63, last63IGPDIConsistency);
            const iry_dolar_consistency_3m = getConsistencyForPeriod(dolarConsistencyReachedLast63, last63DolarConsistency);
            const iry_euro_consistency_3m = getConsistencyForPeriod(euroConsistencyReachedLast63, last63EuroConsistency);

            // Consistency 6M
            const iry_cdi_consistency_6m = getConsistencyForPeriod(cdiConsistencyReachedLast126, last126CDIConsistency);
            const iry_bovespa_consistency_6m = getConsistencyForPeriod(bovespaConsistencyReachedLast126, last126BovespaConsistency);
            const iry_ipca_consistency_6m = getConsistencyForPeriod(ipcaConsistencyReachedLast126, last126IPCAConsistency);
            const iry_igpm_consistency_6m = getConsistencyForPeriod(igpmConsistencyReachedLast126, last126IGPMConsistency);
            const iry_igpdi_consistency_6m = getConsistencyForPeriod(igpdiConsistencyReachedLast126, last126IGPDIConsistency);
            const iry_dolar_consistency_6m = getConsistencyForPeriod(dolarConsistencyReachedLast126, last126DolarConsistency);
            const iry_euro_consistency_6m = getConsistencyForPeriod(euroConsistencyReachedLast126, last126EuroConsistency);

            // Consistency 1Y                    
            const iry_cdi_consistency_1y = getConsistencyForPeriod(cdiConsistencyReachedLast252, last252CDIConsistency);
            const iry_bovespa_consistency_1y = getConsistencyForPeriod(bovespaConsistencyReachedLast252, last252BovespaConsistency);
            const iry_ipca_consistency_1y = getConsistencyForPeriod(ipcaConsistencyReachedLast252, last252IPCAConsistency);
            const iry_igpm_consistency_1y = getConsistencyForPeriod(igpmConsistencyReachedLast252, last252IGPMConsistency);
            const iry_igpdi_consistency_1y = getConsistencyForPeriod(igpdiConsistencyReachedLast252, last252IGPDIConsistency);
            const iry_dolar_consistency_1y = getConsistencyForPeriod(dolarConsistencyReachedLast252, last252DolarConsistency);
            const iry_euro_consistency_1y = getConsistencyForPeriod(euroConsistencyReachedLast252, last252EuroConsistency);

            // Consistency 2Y
            const iry_cdi_consistency_2y = getConsistencyForPeriod(cdiConsistencyReachedLast504, last504CDIConsistency);
            const iry_bovespa_consistency_2y = getConsistencyForPeriod(bovespaConsistencyReachedLast504, last504BovespaConsistency);
            const iry_ipca_consistency_2y = getConsistencyForPeriod(ipcaConsistencyReachedLast504, last504IPCAConsistency);
            const iry_igpm_consistency_2y = getConsistencyForPeriod(igpmConsistencyReachedLast504, last504IGPMConsistency);
            const iry_igpdi_consistency_2y = getConsistencyForPeriod(igpdiConsistencyReachedLast504, last504IGPDIConsistency);
            const iry_dolar_consistency_2y = getConsistencyForPeriod(dolarConsistencyReachedLast504, last504DolarConsistency);
            const iry_euro_consistency_2y = getConsistencyForPeriod(euroConsistencyReachedLast504, last504EuroConsistency);

            // Consistency 3Y
            const iry_cdi_consistency_3y = getConsistencyForPeriod(cdiConsistencyReachedLast756, last756CDIConsistency);
            const iry_bovespa_consistency_3y = getConsistencyForPeriod(bovespaConsistencyReachedLast756, last756BovespaConsistency);
            const iry_ipca_consistency_3y = getConsistencyForPeriod(ipcaConsistencyReachedLast756, last756IPCAConsistency);
            const iry_igpm_consistency_3y = getConsistencyForPeriod(igpmConsistencyReachedLast756, last756IGPMConsistency);
            const iry_igpdi_consistency_3y = getConsistencyForPeriod(igpdiConsistencyReachedLast756, last756IGPDIConsistency);
            const iry_dolar_consistency_3y = getConsistencyForPeriod(dolarConsistencyReachedLast756, last756DolarConsistency);
            const iry_euro_consistency_3y = getConsistencyForPeriod(euroConsistencyReachedLast756, last756EuroConsistency);

            // Networth
            const iry_networth = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).vl_patrim_liq, chunk.vl_patrim_liq);
            const iry_networth_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].vl_patrim_liq, chunk.vl_patrim_liq);
            const iry_networth_ytd = returnFromQuota(chunksByYear[year][0].vl_patrim_liq, chunk.vl_patrim_liq);
            const iry_networth_1m = returnFromQuota(last21Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            const iry_networth_3m = returnFromQuota(last63Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            const iry_networth_6m = returnFromQuota(last126Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            const iry_networth_1y = returnFromQuota(last252Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            const iry_networth_2y = returnFromQuota(last504Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            const iry_networth_3y = returnFromQuota(last756Chunks[0].vl_patrim_liq, chunk.vl_patrim_liq);
            const iry_accumulated_networth = chunk.vl_patrim_liq;

            // Quotaholders
            const iry_quotaholders = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).nr_cotst, chunk.nr_cotst);
            const iry_quotaholders_mtd = returnFromQuota(chunksByYearAndMonth[year][month][0].nr_cotst, chunk.nr_cotst);
            const iry_quotaholders_ytd = returnFromQuota(chunksByYear[year][0].nr_cotst, chunk.nr_cotst);
            const iry_quotaholders_1m = returnFromQuota(last21Chunks[0].nr_cotst, chunk.nr_cotst);
            const iry_quotaholders_3m = returnFromQuota(last63Chunks[0].nr_cotst, chunk.nr_cotst);
            const iry_quotaholders_6m = returnFromQuota(last126Chunks[0].nr_cotst, chunk.nr_cotst);
            const iry_quotaholders_1y = returnFromQuota(last252Chunks[0].nr_cotst, chunk.nr_cotst);
            const iry_quotaholders_2y = returnFromQuota(last504Chunks[0].nr_cotst, chunk.nr_cotst);
            const iry_quotaholders_3y = returnFromQuota(last756Chunks[0].nr_cotst, chunk.nr_cotst);
            const iry_accumulated_quotaholders = chunk.nr_cotst;

            const endOfYear = chunk.dt_comptc.endOf('year');

            const iry_dt_comptc = `${endOfYear.$y}-${string.pad(endOfYear.$M + 1, 2)}-${string.pad(endOfYear.$D, 2)}`;

            if (startSavingFrom == null || iry_dt_comptc >= startSavingFrom) {
                stream.push({
                    table: 'investment_return_yearly',
                    primaryKey: ['iry_CNPJ_FUNDO', 'iry_DT_COMPTC'],
                    fields: {
                        iry_id: uuidv1(),
                        iry_cnpj_fundo: chunk.cnpj_fundo,
                        iry_dt_comptc,
                        iry_investment_return,
                        iry_cdi_investment_return,
                        iry_bovespa_investment_return,
                        iry_ipca_investment_return,
                        iry_igpm_investment_return,
                        iry_igpdi_investment_return,
                        iry_dolar_investment_return,
                        iry_euro_investment_return,
                        iry_investment_return_mtd,
                        iry_cdi_investment_return_mtd,
                        iry_bovespa_investment_return_mtd,
                        iry_ipca_investment_return_mtd,
                        iry_igpm_investment_return_mtd,
                        iry_igpdi_investment_return_mtd,
                        iry_dolar_investment_return_mtd,
                        iry_euro_investment_return_mtd,
                        iry_investment_return_ytd,
                        iry_cdi_investment_return_ytd,
                        iry_bovespa_investment_return_ytd,
                        iry_ipca_investment_return_ytd,
                        iry_igpm_investment_return_ytd,
                        iry_igpdi_investment_return_ytd,
                        iry_dolar_investment_return_ytd,
                        iry_euro_investment_return_ytd,
                        iry_investment_return_1m,
                        iry_cdi_investment_return_1m,
                        iry_bovespa_investment_return_1m,
                        iry_ipca_investment_return_1m,
                        iry_igpm_investment_return_1m,
                        iry_igpdi_investment_return_1m,
                        iry_dolar_investment_return_1m,
                        iry_euro_investment_return_1m,
                        iry_investment_return_3m,
                        iry_cdi_investment_return_3m,
                        iry_bovespa_investment_return_3m,
                        iry_ipca_investment_return_3m,
                        iry_igpm_investment_return_3m,
                        iry_igpdi_investment_return_3m,
                        iry_dolar_investment_return_3m,
                        iry_euro_investment_return_3m,
                        iry_investment_return_6m,
                        iry_cdi_investment_return_6m,
                        iry_bovespa_investment_return_6m,
                        iry_ipca_investment_return_6m,
                        iry_igpm_investment_return_6m,
                        iry_igpdi_investment_return_6m,
                        iry_dolar_investment_return_6m,
                        iry_euro_investment_return_6m,
                        iry_investment_return_1y,
                        iry_cdi_investment_return_1y,
                        iry_bovespa_investment_return_1y,
                        iry_ipca_investment_return_1y,
                        iry_igpm_investment_return_1y,
                        iry_igpdi_investment_return_1y,
                        iry_dolar_investment_return_1y,
                        iry_euro_investment_return_1y,
                        iry_investment_return_2y,
                        iry_cdi_investment_return_2y,
                        iry_bovespa_investment_return_2y,
                        iry_ipca_investment_return_2y,
                        iry_igpm_investment_return_2y,
                        iry_igpdi_investment_return_2y,
                        iry_dolar_investment_return_2y,
                        iry_euro_investment_return_2y,
                        iry_investment_return_3y,
                        iry_cdi_investment_return_3y,
                        iry_bovespa_investment_return_3y,
                        iry_ipca_investment_return_3y,
                        iry_igpm_investment_return_3y,
                        iry_igpdi_investment_return_3y,
                        iry_dolar_investment_return_3y,
                        iry_euro_investment_return_3y,
                        iry_accumulated_investment_return,
                        iry_cdi_accumulated_investment_return,
                        iry_bovespa_accumulated_investment_return,
                        iry_ipca_accumulated_investment_return,
                        iry_igpm_accumulated_investment_return,
                        iry_igpdi_accumulated_investment_return,
                        iry_dolar_accumulated_investment_return,
                        iry_euro_accumulated_investment_return,
                        iry_risk,
                        iry_risk_mtd,
                        iry_risk_ytd,
                        iry_risk_1m,
                        iry_risk_3m,
                        iry_risk_6m,
                        iry_risk_1y,
                        iry_risk_2y,
                        iry_risk_3y,
                        iry_accumulated_risk,
                        iry_cdi_sharpe,
                        iry_bovespa_sharpe,
                        iry_ipca_sharpe,
                        iry_igpm_sharpe,
                        iry_igpdi_sharpe,
                        iry_dolar_sharpe,
                        iry_euro_sharpe,
                        iry_cdi_sharpe_mtd,
                        iry_bovespa_sharpe_mtd,
                        iry_ipca_sharpe_mtd,
                        iry_igpm_sharpe_mtd,
                        iry_igpdi_sharpe_mtd,
                        iry_dolar_sharpe_mtd,
                        iry_euro_sharpe_mtd,
                        iry_cdi_sharpe_ytd,
                        iry_bovespa_sharpe_ytd,
                        iry_ipca_sharpe_ytd,
                        iry_igpm_sharpe_ytd,
                        iry_igpdi_sharpe_ytd,
                        iry_dolar_sharpe_ytd,
                        iry_euro_sharpe_ytd,
                        iry_cdi_sharpe_1m,
                        iry_bovespa_sharpe_1m,
                        iry_ipca_sharpe_1m,
                        iry_igpm_sharpe_1m,
                        iry_igpdi_sharpe_1m,
                        iry_dolar_sharpe_1m,
                        iry_euro_sharpe_1m,
                        iry_cdi_sharpe_3m,
                        iry_bovespa_sharpe_3m,
                        iry_ipca_sharpe_3m,
                        iry_igpm_sharpe_3m,
                        iry_igpdi_sharpe_3m,
                        iry_dolar_sharpe_3m,
                        iry_euro_sharpe_3m,
                        iry_cdi_sharpe_6m,
                        iry_bovespa_sharpe_6m,
                        iry_ipca_sharpe_6m,
                        iry_igpm_sharpe_6m,
                        iry_igpdi_sharpe_6m,
                        iry_dolar_sharpe_6m,
                        iry_euro_sharpe_6m,
                        iry_cdi_sharpe_1y,
                        iry_bovespa_sharpe_1y,
                        iry_ipca_sharpe_1y,
                        iry_igpm_sharpe_1y,
                        iry_igpdi_sharpe_1y,
                        iry_dolar_sharpe_1y,
                        iry_euro_sharpe_1y,
                        iry_cdi_sharpe_2y,
                        iry_bovespa_sharpe_2y,
                        iry_ipca_sharpe_2y,
                        iry_igpm_sharpe_2y,
                        iry_igpdi_sharpe_2y,
                        iry_dolar_sharpe_2y,
                        iry_euro_sharpe_2y,
                        iry_cdi_sharpe_3y,
                        iry_bovespa_sharpe_3y,
                        iry_ipca_sharpe_3y,
                        iry_igpm_sharpe_3y,
                        iry_igpdi_sharpe_3y,
                        iry_dolar_sharpe_3y,
                        iry_euro_sharpe_3y,
                        iry_cdi_accumulated_sharpe,
                        iry_bovespa_accumulated_sharpe,
                        iry_ipca_accumulated_sharpe,
                        iry_igpm_accumulated_sharpe,
                        iry_igpdi_accumulated_sharpe,
                        iry_dolar_accumulated_sharpe,
                        iry_euro_accumulated_sharpe,
                        iry_cdi_consistency_mtd,
                        iry_bovespa_consistency_mtd,
                        iry_ipca_consistency_mtd,
                        iry_igpm_consistency_mtd,
                        iry_igpdi_consistency_mtd,
                        iry_dolar_consistency_mtd,
                        iry_euro_consistency_mtd,
                        iry_cdi_consistency_ytd,
                        iry_bovespa_consistency_ytd,
                        iry_ipca_consistency_ytd,
                        iry_igpm_consistency_ytd,
                        iry_igpdi_consistency_ytd,
                        iry_dolar_consistency_ytd,
                        iry_euro_consistency_ytd,
                        iry_cdi_consistency_1m,
                        iry_bovespa_consistency_1m,
                        iry_ipca_consistency_1m,
                        iry_igpm_consistency_1m,
                        iry_igpdi_consistency_1m,
                        iry_dolar_consistency_1m,
                        iry_euro_consistency_1m,
                        iry_cdi_consistency_3m,
                        iry_bovespa_consistency_3m,
                        iry_ipca_consistency_3m,
                        iry_igpm_consistency_3m,
                        iry_igpdi_consistency_3m,
                        iry_dolar_consistency_3m,
                        iry_euro_consistency_3m,
                        iry_cdi_consistency_6m,
                        iry_bovespa_consistency_6m,
                        iry_ipca_consistency_6m,
                        iry_igpm_consistency_6m,
                        iry_igpdi_consistency_6m,
                        iry_dolar_consistency_6m,
                        iry_euro_consistency_6m,
                        iry_cdi_consistency_1y,
                        iry_bovespa_consistency_1y,
                        iry_ipca_consistency_1y,
                        iry_igpm_consistency_1y,
                        iry_igpdi_consistency_1y,
                        iry_dolar_consistency_1y,
                        iry_euro_consistency_1y,
                        iry_cdi_consistency_2y,
                        iry_bovespa_consistency_2y,
                        iry_ipca_consistency_2y,
                        iry_igpm_consistency_2y,
                        iry_igpdi_consistency_2y,
                        iry_dolar_consistency_2y,
                        iry_euro_consistency_2y,
                        iry_cdi_consistency_3y,
                        iry_bovespa_consistency_3y,
                        iry_ipca_consistency_3y,
                        iry_igpm_consistency_3y,
                        iry_igpdi_consistency_3y,
                        iry_dolar_consistency_3y,
                        iry_euro_consistency_3y,
                        iry_networth,
                        iry_accumulated_networth,
                        iry_networth_mtd,
                        iry_networth_ytd,
                        iry_networth_1m,
                        iry_networth_3m,
                        iry_networth_6m,
                        iry_networth_1y,
                        iry_networth_2y,
                        iry_networth_3y,
                        iry_quotaholders,
                        iry_accumulated_quotaholders,
                        iry_quotaholders_mtd,
                        iry_quotaholders_ytd,
                        iry_quotaholders_1m,
                        iry_quotaholders_3m,
                        iry_quotaholders_6m,
                        iry_quotaholders_1y,
                        iry_quotaholders_2y,
                        iry_quotaholders_3y
                    }
                });
            }
        }
    };

    return stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: function (chunk, e, callback) {
            try {

                if (chunk.dt_comptc == null) {
                    callback();
                    return;
                }

                const quotize = (quotaField, quotaValor) => chunks.length == 0 ? chunk[quotaField] = 1 : chunk[quotaField] = (chunks[chunks.length - 1][quotaField] * (1 + (chunks[chunks.length - 1][quotaValor] / 100)));
                const fillNull = (field) => chunk[field] = chunk[field] == null ? (chunks.length == 0 ? 0 : chunks[chunks.length - 1][field]) : chunk[field];

                // Work some fields
                quotize('cdi_quota', 'cdi_valor');
                chunk.ipca_valor = returnFromMonthlyTax(chunk.ipca_valor);
                quotize('ipca_quota', 'ipca_valor');
                chunk.igpm_valor = returnFromMonthlyTax(chunk.igpm_valor);
                quotize('igpm_quota', 'igpm_valor');
                chunk.igpdi_valor = returnFromMonthlyTax(chunk.igpdi_valor);
                quotize('igpdi_quota', 'igpdi_valor');
                fillNull('bovespa_valor');

                chunks.push(chunk);
                if (last21Chunks.length >= 21) last21Chunks.shift();
                last21Chunks.push(chunk);
                if (last63Chunks.length >= 63) last63Chunks.shift();
                last63Chunks.push(chunk);
                if (last126Chunks.length >= 126) last126Chunks.shift();
                last126Chunks.push(chunk);
                if (last252Chunks.length >= 252) last252Chunks.shift();
                last252Chunks.push(chunk);
                if (last504Chunks.length >= 504) last504Chunks.shift();
                last504Chunks.push(chunk);
                if (last756Chunks.length >= 756) last756Chunks.shift();
                last756Chunks.push(chunk);

                const month = getMonthHash(chunk.dt_comptc);
                const year = getYearHash(chunk.dt_comptc);

                if (!chunksByYear[year]) chunksByYear[year] = [];
                chunksByYear[year].push(chunk);

                if (!chunksByYearAndMonth[year]) chunksByYearAndMonth[year] = {};
                if (!chunksByYearAndMonth[year][month]) chunksByYearAndMonth[year][month] = [];
                chunksByYearAndMonth[year][month].push(chunk);

                if (!lastMTDCDIConsistency[month]) lastMTDCDIConsistency[month] = [];
                if (!lastMTDBovespaConsistency[month]) lastMTDBovespaConsistency[month] = [];
                if (!lastMTDIPCAConsistency[month]) lastMTDIPCAConsistency[month] = [];
                if (!lastMTDIGPMConsistency[month]) lastMTDIGPMConsistency[month] = [];
                if (!lastMTDIGPDIConsistency[month]) lastMTDIGPDIConsistency[month] = [];
                if (!lastMTDDolarConsistency[month]) lastMTDDolarConsistency[month] = [];
                if (!lastMTDEuroConsistency[month]) lastMTDEuroConsistency[month] = [];
                if (!cdiConsistencyReachedMTD[month]) cdiConsistencyReachedMTD[month] = 0;
                if (!bovespaConsistencyReachedMTD[month]) bovespaConsistencyReachedMTD[month] = 0;
                if (!ipcaConsistencyReachedMTD[month]) ipcaConsistencyReachedMTD[month] = 0;
                if (!igpmConsistencyReachedMTD[month]) igpmConsistencyReachedMTD[month] = 0;
                if (!igpdiConsistencyReachedMTD[month]) igpdiConsistencyReachedMTD[month] = 0;
                if (!dolarConsistencyReachedMTD[month]) dolarConsistencyReachedMTD[month] = 0;
                if (!euroConsistencyReachedMTD[month]) euroConsistencyReachedMTD[month] = 0;

                if (!lastYTDCDIConsistency[year]) lastYTDCDIConsistency[year] = [];
                if (!lastYTDBovespaConsistency[year]) lastYTDBovespaConsistency[year] = [];
                if (!lastYTDIPCAConsistency[year]) lastYTDIPCAConsistency[year] = [];
                if (!lastYTDIGPMConsistency[year]) lastYTDIGPMConsistency[year] = [];
                if (!lastYTDIGPDIConsistency[year]) lastYTDIGPDIConsistency[year] = [];
                if (!lastYTDDolarConsistency[year]) lastYTDDolarConsistency[year] = [];
                if (!lastYTDEuroConsistency[year]) lastYTDEuroConsistency[year] = [];
                if (!cdiConsistencyReachedYTD[year]) cdiConsistencyReachedYTD[year] = 0;
                if (!bovespaConsistencyReachedYTD[year]) bovespaConsistencyReachedYTD[year] = 0;
                if (!ipcaConsistencyReachedYTD[year]) ipcaConsistencyReachedYTD[year] = 0;
                if (!igpmConsistencyReachedYTD[year]) igpmConsistencyReachedYTD[year] = 0;
                if (!igpdiConsistencyReachedYTD[year]) igpdiConsistencyReachedYTD[year] = 0;
                if (!dolarConsistencyReachedYTD[year]) dolarConsistencyReachedYTD[year] = 0;
                if (!euroConsistencyReachedYTD[year]) euroConsistencyReachedYTD[year] = 0;

                if (lastChunk != null) {
                    // Monthly
                    if (lastChunk.dt_comptc.month() != chunk.dt_comptc.month()) {
                        processMonthly(this, lastChunk);
                    }

                    // Yearly
                    if (lastChunk.dt_comptc.year() != chunk.dt_comptc.year()) {
                        processYearly(this, lastChunk);
                    }

                }
                // Daily
                processDaily(this, chunk, lastChunk);

                lastChunk = chunk;
                callback();
            } catch (ex) {                
                callback(ex);
            }
        },
        flush: function (callback) {
            try {
                if (lastChunk != null) {
                    // Monthly
                    processMonthly(this, lastChunk);

                    // Yearly
                    processYearly(this, lastChunk);
                }

                callback();
            } catch (ex) {                
                callback(ex);
            }
        }
    });
};

module.exports = createCalculatorStream;