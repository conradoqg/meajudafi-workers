const uuidv1 = require('uuid/v1');
const stream = require('stream');
const CONFIG = require('../config');
const StandardDeviation = require('./../util/standardDeviation');

const createCalculatorStream = () => {
    let lastChunk = null;
    let chunks = [];
    let chunksByDate = {};
    let last252Chunks = [];
    let last504Chunks = [];
    let last756Chunks = [];
    let last252CDIConsistency = [];
    let last504CDIConsistency = [];
    let last756CDIConsistency = [];
    let last252BovespaConsistency = [];
    let last504BovespaConsistency = [];
    let last756BovespaConsistency = [];
    let cdiConsistencyReachedLast252 = 0;
    let cdiConsistencyReachedLast504 = 0;
    let cdiConsistencyReachedLast756 = 0;
    let bovespaConsistencyReachedLast252 = 0;
    let bovespaConsistencyReachedLast504 = 0;
    let bovespaConsistencyReachedLast756 = 0;
    let risk = null;
    let risksByMonth = {};
    let risksByYear = {};
    let last252Risk = null;
    let last504Risk = null;
    let last756Risk = null;

    const returnFromQuota = (initialQuota, finalQuota) => (finalQuota == 0) ? 0 : (finalQuota / initialQuota) - 1;
    const calcSharpeForPeriod = (risk, investment_return, cdi_investment_return, length) => {
        if (risk == 0) return 0;
        const annualizedAccInvestmentReturn = ((investment_return / length) * 252);
        const annualizedAccCDIInvestmentReturn = ((cdi_investment_return / length) * 252);
        return (annualizedAccInvestmentReturn - annualizedAccCDIInvestmentReturn) / risk;
    };
    const calcConsistencyForPeriod = (investment_return, cdi_investment_return, period, consistencyReached, lastConsistency) => {
        let consistencyPoint = 0;
        if (investment_return > cdi_investment_return) consistencyPoint = 1;
        if (lastConsistency.length >= period) consistencyReached -= lastConsistency.shift();
        consistencyReached += consistencyPoint;
        lastConsistency.push(consistencyPoint);
        return consistencyReached;
    };
    const getConsistencyForPeriod = (consistencyReached, lastConsistency) => {
        return ((100 * consistencyReached) / lastConsistency.length) / 100;
    };
    const getMonthHash = (momentDate) => momentDate.format('YYYY-MM');
    const getYearHash = (momentDate) => momentDate.format('YYYY');
    const lastItem = (array) => array[array.length - 1];
    const firstItem = (array) => array[0];
    const lastMonthOrFirstDay = (chunk, year, month, lastYear, lastMonth) => {
        if (chunksByDate[lastYear] && chunksByDate[lastYear][lastMonth]) return lastItem(chunksByDate[lastYear][lastMonth]);
        else return firstItem(chunksByDate[year][month]);
    };
    const lastYearOrFirstDay = (chunk, year, lastYear) => {
        if (chunksByDate[lastYear]) return lastItem(lastItem(Object.values(chunksByDate[lastYear])));
        else return firstItem(firstItem(Object.values(chunksByDate[year])));
    };

    const processDaily = (stream, chunk, chunkBefore) => {
        let ird_investment_return = 0;
        let ird_cdi_investment_return = 0;
        let ird_bovespa_investment_return = 0;
        let ird_investment_return_1y = 0;
        let ird_cdi_investment_return_1y = 0;
        let ird_bovespa_investment_return_1y = 0;
        let ird_investment_return_2y = 0;
        let ird_cdi_investment_return_2y = 0;
        let ird_bovespa_investment_return_2y = 0;
        let ird_investment_return_3y = 0;
        let ird_cdi_investment_return_3y = 0;
        let ird_bovespa_investment_return_3y = 0;
        let ird_accumulated_investment_return = 0;
        let ird_cdi_accumulated_investment_return = 0;
        let ird_bovespa_accumulated_investment_return = 0;
        let ird_risk_1y = 0;
        let ird_risk_2y = 0;
        let ird_risk_3y = 0;
        let ird_accumulated_risk = 0;
        let ird_cdi_sharpe_1y = 0;
        let ird_cdi_sharpe_2y = 0;
        let ird_cdi_sharpe_3y = 0;
        let ird_cdi_accumulated_sharpe = 0;
        let ird_cdi_consistency_1y = 0;
        let ird_cdi_consistency_2y = 0;
        let ird_cdi_consistency_3y = 0;
        let ird_networth = 0;
        let ird_quotaholders = 0;
        let ird_bovespa_sharpe_1y = 0;
        let ird_bovespa_sharpe_2y = 0;
        let ird_bovespa_sharpe_3y = 0;
        let ird_bovespa_accumulated_sharpe = 0;
        let ird_bovespa_consistency_1y = 0;
        let ird_bovespa_consistency_2y = 0;
        let ird_bovespa_consistency_3y = 0;

        if (chunkBefore) {
            let change = returnFromQuota(chunkBefore.vl_quota, chunk.vl_quota);
            let cdiChange = returnFromQuota(chunkBefore.cdi_quota, chunk.cdi_quota);
            let bovespaChange = returnFromQuota(chunkBefore.bovespa_valor, chunk.bovespa_valor);
            let month = getMonthHash(chunk.dt_comptc);
            let year = getYearHash(chunk.dt_comptc);

            // Return
            ird_investment_return = change;
            ird_cdi_investment_return = cdiChange;
            ird_bovespa_investment_return = bovespaChange;

            // Return 1Y
            ird_investment_return_1y = returnFromQuota(last252Chunks[0].vl_quota, chunk.vl_quota);
            ird_cdi_investment_return_1y = returnFromQuota(last252Chunks[0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_investment_return_1y = returnFromQuota(last252Chunks[0].bovespa_valor, chunk.bovespa_valor);

            // Return 2Y        
            ird_investment_return_2y = returnFromQuota(last504Chunks[0].vl_quota, chunk.vl_quota);
            ird_cdi_investment_return_2y = returnFromQuota(last504Chunks[0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_investment_return_2y = returnFromQuota(last504Chunks[0].bovespa_valor, chunk.bovespa_valor);

            // Return 3Y        
            ird_investment_return_3y = returnFromQuota(last756Chunks[0].vl_quota, chunk.vl_quota);
            ird_cdi_investment_return_3y = returnFromQuota(last756Chunks[0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_investment_return_3y = returnFromQuota(last756Chunks[0].bovespa_valor, chunk.bovespa_valor);

            // Accumulated Return        
            ird_accumulated_investment_return = returnFromQuota(chunks[0].vl_quota, chunk.vl_quota);
            ird_cdi_accumulated_investment_return = returnFromQuota(chunks[0].cdi_quota, chunk.cdi_quota);
            ird_bovespa_accumulated_investment_return = returnFromQuota(chunks[0].bovespa_valor, chunk.bovespa_valor);

            // Months Risk
            if (!risksByMonth[month]) risksByMonth[month] = new StandardDeviation(change);
            else risksByMonth[month].addMeasurement(change);

            // Years Risk
            if (!risksByYear[year]) risksByYear[year] = new StandardDeviation(change);
            else risksByYear[year].addMeasurement(change);

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

            // Sharpe 1Y     
            ird_cdi_sharpe_1y = calcSharpeForPeriod(ird_risk_1y, ird_investment_return_1y, ird_cdi_investment_return_1y, last252Chunks.length - 1);
            ird_bovespa_sharpe_1y = calcSharpeForPeriod(ird_risk_1y, ird_investment_return_1y, ird_bovespa_investment_return_1y, last252Chunks.length - 1);

            // Sharpe 2Y                
            ird_cdi_sharpe_2y = calcSharpeForPeriod(ird_risk_2y, ird_investment_return_2y, ird_cdi_investment_return_2y, last504Chunks.length - 1);
            ird_bovespa_sharpe_2y = calcSharpeForPeriod(ird_risk_2y, ird_investment_return_2y, ird_bovespa_investment_return_2y, last504Chunks.length - 1);

            // Sharpe 3Y        
            ird_cdi_sharpe_3y = calcSharpeForPeriod(ird_risk_3y, ird_investment_return_3y, ird_cdi_investment_return_3y, last756Chunks.length - 1);
            ird_bovespa_sharpe_3y = calcSharpeForPeriod(ird_risk_3y, ird_investment_return_3y, ird_bovespa_investment_return_3y, last756Chunks.length - 1);

            // Accumulated Sharpe        
            ird_cdi_accumulated_sharpe = calcSharpeForPeriod(ird_accumulated_risk, ird_accumulated_investment_return, ird_cdi_accumulated_investment_return, chunks.length - 1);
            ird_bovespa_accumulated_sharpe = calcSharpeForPeriod(ird_accumulated_risk, ird_accumulated_investment_return, ird_bovespa_accumulated_investment_return, chunks.length - 1);

            // Consistency 1Y        
            cdiConsistencyReachedLast252 = calcConsistencyForPeriod(ird_investment_return_1y, ird_cdi_investment_return_1y, 252, cdiConsistencyReachedLast252, last252CDIConsistency);
            ird_cdi_consistency_1y = getConsistencyForPeriod(cdiConsistencyReachedLast252, last252CDIConsistency);
            bovespaConsistencyReachedLast252 = calcConsistencyForPeriod(ird_investment_return_1y, ird_bovespa_investment_return_1y, 252, bovespaConsistencyReachedLast252, last252BovespaConsistency);
            ird_bovespa_consistency_1y = getConsistencyForPeriod(bovespaConsistencyReachedLast252, last252BovespaConsistency);

            // Consistency 2Y
            cdiConsistencyReachedLast504 = calcConsistencyForPeriod(ird_investment_return_2y, ird_cdi_investment_return_2y, 504, cdiConsistencyReachedLast504, last504CDIConsistency);
            ird_cdi_consistency_2y = getConsistencyForPeriod(cdiConsistencyReachedLast504, last504CDIConsistency);
            bovespaConsistencyReachedLast504 = calcConsistencyForPeriod(ird_investment_return_2y, ird_bovespa_investment_return_2y, 504, bovespaConsistencyReachedLast504, last504BovespaConsistency);
            ird_bovespa_consistency_2y = getConsistencyForPeriod(bovespaConsistencyReachedLast504, last504BovespaConsistency);

            // Consistency 3Y
            cdiConsistencyReachedLast756 = calcConsistencyForPeriod(ird_investment_return_3y, ird_cdi_investment_return_3y, 756, cdiConsistencyReachedLast756, last756CDIConsistency);
            ird_cdi_consistency_3y = getConsistencyForPeriod(cdiConsistencyReachedLast756, last756CDIConsistency);
            bovespaConsistencyReachedLast756 = calcConsistencyForPeriod(ird_investment_return_3y, ird_bovespa_investment_return_3y, 756, bovespaConsistencyReachedLast756, last756BovespaConsistency);
            ird_bovespa_consistency_3y = getConsistencyForPeriod(bovespaConsistencyReachedLast756, last756BovespaConsistency);

            ird_networth = chunk.vl_patrim_liq;
            ird_quotaholders = chunk.nr_cotst;
        }
        stream.push({
            table: 'investment_return_daily',
            primaryKey: ['ird_CNPJ_FUNDO', 'ird_DT_COMPTC'],
            fields: {
                ird_id: uuidv1(),
                ird_cnpj_fundo: chunk.cnpj_fundo,
                ird_dt_comptc: chunk.dt_comptc.format('YYYY-MM-DD'),
                ird_investment_return,
                ird_investment_return_1y,
                ird_investment_return_2y,
                ird_investment_return_3y,
                ird_accumulated_investment_return,
                ird_risk_1y,
                ird_risk_2y,
                ird_risk_3y,
                ird_accumulated_risk,
                ird_cdi_sharpe_1y,
                ird_cdi_sharpe_2y,
                ird_cdi_sharpe_3y,
                ird_cdi_accumulated_sharpe,
                ird_cdi_consistency_1y,
                ird_cdi_consistency_2y,
                ird_cdi_consistency_3y,
                ird_networth,
                ird_quotaholders,
                ird_cdi_investment_return,
                ird_cdi_investment_return_1y,
                ird_cdi_investment_return_2y,
                ird_cdi_investment_return_3y,
                ird_cdi_accumulated_investment_return,
                ird_bovespa_investment_return,
                ird_bovespa_investment_return_1y,
                ird_bovespa_investment_return_2y,
                ird_bovespa_investment_return_3y,
                ird_bovespa_accumulated_investment_return,
                ird_bovespa_sharpe_1y,
                ird_bovespa_sharpe_2y,
                ird_bovespa_sharpe_3y,
                ird_bovespa_accumulated_sharpe,
                ird_bovespa_consistency_1y,
                ird_bovespa_consistency_2y,
                ird_bovespa_consistency_3y
            }
        });
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

            // Return 1Y            
            const irm_investment_return_1y = returnFromQuota(last252Chunks[0].vl_quota, chunk.vl_quota);
            const irm_cdi_investment_return_1y = returnFromQuota(last252Chunks[0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_investment_return_1y = returnFromQuota(last252Chunks[0].bovespa_valor, chunk.bovespa_valor);

            // Return 2Y
            const irm_investment_return_2y = returnFromQuota(last504Chunks[0].vl_quota, chunk.vl_quota);
            const irm_cdi_investment_return_2y = returnFromQuota(last504Chunks[0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_investment_return_2y = returnFromQuota(last504Chunks[0].bovespa_valor, chunk.bovespa_valor);

            // Return 3Y
            const irm_investment_return_3y = returnFromQuota(last756Chunks[0].vl_quota, chunk.vl_quota);
            const irm_cdi_investment_return_3y = returnFromQuota(last756Chunks[0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_investment_return_3y = returnFromQuota(last756Chunks[0].bovespa_valor, chunk.bovespa_valor);

            // Accumulated Return
            const irm_accumulated_investment_return = returnFromQuota(chunks[0].vl_quota, chunk.vl_quota);
            const irm_cdi_accumulated_investment_return = returnFromQuota(chunks[0].cdi_quota, chunk.cdi_quota);
            const irm_bovespa_accumulated_investment_return = returnFromQuota(chunks[0].bovespa_valor, chunk.bovespa_valor);

            // Months Risk            
            const irm_risk = risksByMonth[month].get() * Math.sqrt(252);

            // Risk 1Y            
            const irm_risk_1y = last252Risk.get() * Math.sqrt(252);

            // Risk 2Y            
            const irm_risk_2y = last504Risk.get() * Math.sqrt(252);

            // Risk 3Y            
            const irm_risk_3y = last756Risk.get() * Math.sqrt(252);

            // Accumulated Risk
            const irm_accumulated_risk = risk.get() * Math.sqrt(252);

            // Sharpe
            const irm_cdi_sharpe = calcSharpeForPeriod(irm_risk, irm_investment_return, irm_cdi_investment_return, chunksByDate[year][month].length);
            const irm_bovespa_sharpe = calcSharpeForPeriod(irm_risk, irm_investment_return, irm_bovespa_investment_return, chunksByDate[year][month].length);

            // Sharpe 1Y
            const irm_cdi_sharpe_1y = calcSharpeForPeriod(irm_risk_1y, irm_investment_return_1y, irm_cdi_investment_return_1y, last252Chunks.length - 1);
            const irm_bovespa_sharpe_1y = calcSharpeForPeriod(irm_risk_1y, irm_investment_return_1y, irm_bovespa_investment_return_1y, last252Chunks.length - 1);

            // Sharpe 2Y
            const irm_cdi_sharpe_2y = calcSharpeForPeriod(irm_risk_2y, irm_investment_return_2y, irm_cdi_investment_return_2y, last504Chunks.length - 1);
            const irm_bovespa_sharpe_2y = calcSharpeForPeriod(irm_risk_2y, irm_investment_return_2y, irm_bovespa_investment_return_2y, last504Chunks.length - 1);

            // Sharpe 3Y
            const irm_cdi_sharpe_3y = calcSharpeForPeriod(irm_risk_3y, irm_investment_return_3y, irm_cdi_investment_return_3y, last756Chunks.length - 1);
            const irm_bovespa_sharpe_3y = calcSharpeForPeriod(irm_risk_3y, irm_investment_return_3y, irm_bovespa_investment_return_3y, last756Chunks.length - 1);

            // Accumulated Sharpe            
            const irm_cdi_accumulated_sharpe = calcSharpeForPeriod(irm_accumulated_risk, irm_accumulated_investment_return, irm_cdi_accumulated_investment_return, chunks.length - 1);
            const irm_bovespa_accumulated_sharpe = calcSharpeForPeriod(irm_accumulated_risk, irm_accumulated_investment_return, irm_bovespa_accumulated_investment_return, chunks.length - 1);

            // Consistency 1Y        
            const irm_cdi_consistency_1y = getConsistencyForPeriod(cdiConsistencyReachedLast252, last252CDIConsistency);
            const irm_bovespa_consistency_1y = getConsistencyForPeriod(bovespaConsistencyReachedLast252, last252BovespaConsistency);

            // Consistency 2Y
            const irm_cdi_consistency_2y = getConsistencyForPeriod(cdiConsistencyReachedLast504, last504CDIConsistency);
            const irm_bovespa_consistency_2y = getConsistencyForPeriod(bovespaConsistencyReachedLast504, last504BovespaConsistency);

            // Consistency 3Y
            const irm_cdi_consistency_3y = getConsistencyForPeriod(cdiConsistencyReachedLast756, last756CDIConsistency);
            const irm_bovespa_consistency_3y = getConsistencyForPeriod(bovespaConsistencyReachedLast756, last756BovespaConsistency);

            const irm_networth = chunk.vl_patrim_liq;
            const irm_quotaholders = chunk.nr_cotst;

            stream.push({
                table: 'investment_return_monthly',
                primaryKey: ['irm_CNPJ_FUNDO', 'irm_DT_COMPTC'],
                fields: {
                    irm_id: uuidv1(),
                    irm_cnpj_fundo: chunk.cnpj_fundo,
                    irm_dt_comptc: chunk.dt_comptc.endOf('month').format('YYYY-MM-DD'),
                    irm_investment_return,
                    irm_investment_return_1y,
                    irm_investment_return_2y,
                    irm_investment_return_3y,
                    irm_accumulated_investment_return,
                    irm_risk,
                    irm_risk_1y,
                    irm_risk_2y,
                    irm_risk_3y,
                    irm_accumulated_risk,
                    irm_cdi_sharpe,
                    irm_cdi_sharpe_1y,
                    irm_cdi_sharpe_2y,
                    irm_cdi_sharpe_3y,
                    irm_cdi_accumulated_sharpe,
                    irm_cdi_consistency_1y,
                    irm_cdi_consistency_2y,
                    irm_cdi_consistency_3y,
                    irm_networth,
                    irm_quotaholders,
                    irm_cdi_investment_return,
                    irm_cdi_investment_return_1y,
                    irm_cdi_investment_return_2y,
                    irm_cdi_investment_return_3y,
                    irm_cdi_accumulated_investment_return,
                    irm_bovespa_investment_return,
                    irm_bovespa_investment_return_1y,
                    irm_bovespa_investment_return_2y,
                    irm_bovespa_investment_return_3y,
                    irm_bovespa_accumulated_investment_return,
                    irm_bovespa_sharpe,
                    irm_bovespa_sharpe_1y,
                    irm_bovespa_sharpe_2y,
                    irm_bovespa_sharpe_3y,
                    irm_bovespa_accumulated_sharpe,
                    irm_bovespa_consistency_1y,
                    irm_bovespa_consistency_2y,
                    irm_bovespa_consistency_3y
                }
            });
        }
    };

    const processYearly = (stream, chunk) => {
        const lastYear = getYearHash(chunk.dt_comptc.subtract(1, 'year'));
        const year = getYearHash(chunk.dt_comptc);

        if (risk) {
            // Return            
            const iry_investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return = returnFromQuota(lastYearOrFirstDay(chunk, year, lastYear).bovespa_valor, chunk.bovespa_valor);

            // Return 1Y            
            const iry_investment_return_1y = returnFromQuota(last252Chunks[0].vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return_1y = returnFromQuota(last252Chunks[0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return_1y = returnFromQuota(last252Chunks[0].bovespa_valor, chunk.bovespa_valor);

            // Return 2Y
            const iry_investment_return_2y = returnFromQuota(last504Chunks[0].vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return_2y = returnFromQuota(last504Chunks[0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return_2y = returnFromQuota(last504Chunks[0].bovespa_valor, chunk.bovespa_valor);

            // Return 3Y
            const iry_investment_return_3y = returnFromQuota(last756Chunks[0].vl_quota, chunk.vl_quota);
            const iry_cdi_investment_return_3y = returnFromQuota(last756Chunks[0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_investment_return_3y = returnFromQuota(last756Chunks[0].bovespa_valor, chunk.bovespa_valor);

            // Accumulated Return            
            const iry_accumulated_investment_return = returnFromQuota(chunks[0].vl_quota, chunk.vl_quota);
            const iry_cdi_accumulated_investment_return = returnFromQuota(chunks[0].cdi_quota, chunk.cdi_quota);
            const iry_bovespa_accumulated_investment_return = returnFromQuota(chunks[0].bovespa_valor, chunk.bovespa_valor);

            // Risk            
            const iry_risk = risksByYear[year].get() * Math.sqrt(252);

            // Risk 1Y            
            const iry_risk_1y = last252Risk.get() * Math.sqrt(252);

            // Risk 2Y            
            const iry_risk_2y = last504Risk.get() * Math.sqrt(252);

            // Risk 3Y            
            const iry_risk_3y = last756Risk.get() * Math.sqrt(252);

            // Accumulated Risk
            const iry_accumulated_risk = risk.get() * Math.sqrt(252);

            // Sharpe
            const iry_cdi_sharpe = calcSharpeForPeriod(iry_risk, iry_investment_return, iry_cdi_investment_return, Object.values(chunksByDate[year]).reduce((acc, month) => acc + month.length, 0));
            const iry_bovespa_sharpe = calcSharpeForPeriod(iry_risk, iry_investment_return, iry_bovespa_investment_return, Object.values(chunksByDate[year]).reduce((acc, month) => acc + month.length, 0));

            // Sharpe 1Y
            const iry_cdi_sharpe_1y = calcSharpeForPeriod(iry_risk_1y, iry_investment_return_1y, iry_cdi_investment_return_1y, last252Chunks.length - 1);
            const iry_bovespa_sharpe_1y = calcSharpeForPeriod(iry_risk_1y, iry_investment_return_1y, iry_bovespa_investment_return_1y, last252Chunks.length - 1);

            // Sharpe 2Y
            const iry_cdi_sharpe_2y = calcSharpeForPeriod(iry_risk_2y, iry_investment_return_2y, iry_cdi_investment_return_2y, last504Chunks.length - 1);
            const iry_bovespa_sharpe_2y = calcSharpeForPeriod(iry_risk_2y, iry_investment_return_2y, iry_bovespa_investment_return_2y, last504Chunks.length - 1);

            // Sharpe 3Y
            const iry_cdi_sharpe_3y = calcSharpeForPeriod(iry_risk_3y, iry_investment_return_3y, iry_cdi_investment_return_3y, last756Chunks.length - 1);
            const iry_bovespa_sharpe_3y = calcSharpeForPeriod(iry_risk_3y, iry_investment_return_3y, iry_bovespa_investment_return_3y, last756Chunks.length - 1);

            // Accumulated Sharpe            
            const iry_cdi_accumulated_sharpe = calcSharpeForPeriod(iry_accumulated_risk, iry_accumulated_investment_return, iry_cdi_accumulated_investment_return, chunks.length - 1);
            const iry_bovespa_accumulated_sharpe = calcSharpeForPeriod(iry_accumulated_risk, iry_accumulated_investment_return, iry_bovespa_accumulated_investment_return, chunks.length - 1);

            // Consistency 1Y                    
            const iry_cdi_consistency_1y = getConsistencyForPeriod(cdiConsistencyReachedLast252, last252CDIConsistency);
            const iry_bovespa_consistency_1y = getConsistencyForPeriod(bovespaConsistencyReachedLast252, last252BovespaConsistency);

            // Consistency 2Y
            const iry_cdi_consistency_2y = getConsistencyForPeriod(cdiConsistencyReachedLast504, last504CDIConsistency);
            const iry_bovespa_consistency_2y = getConsistencyForPeriod(bovespaConsistencyReachedLast504, last504BovespaConsistency);

            // Consistency 3Y
            const iry_cdi_consistency_3y = getConsistencyForPeriod(cdiConsistencyReachedLast756, last756CDIConsistency);
            const iry_bovespa_consistency_3y = getConsistencyForPeriod(bovespaConsistencyReachedLast756, last756BovespaConsistency);

            const iry_networth = chunk.vl_patrim_liq;
            const iry_quotaholders = chunk.nr_cotst;

            stream.push({
                table: 'investment_return_yearly',
                primaryKey: ['iry_CNPJ_FUNDO', 'iry_DT_COMPTC'],
                fields: {
                    iry_id: uuidv1(),
                    iry_cnpj_fundo: chunk.cnpj_fundo,
                    iry_dt_comptc: chunk.dt_comptc.endOf('year').format('YYYY-MM-DD'),
                    iry_investment_return,
                    iry_investment_return_1y,
                    iry_investment_return_2y,
                    iry_investment_return_3y,
                    iry_accumulated_investment_return,
                    iry_risk,
                    iry_risk_1y,
                    iry_risk_2y,
                    iry_risk_3y,
                    iry_accumulated_risk,
                    iry_cdi_sharpe,
                    iry_cdi_sharpe_1y,
                    iry_cdi_sharpe_2y,
                    iry_cdi_sharpe_3y,
                    iry_cdi_accumulated_sharpe,
                    iry_cdi_consistency_1y,
                    iry_cdi_consistency_2y,
                    iry_cdi_consistency_3y,
                    iry_networth,
                    iry_quotaholders,
                    iry_cdi_investment_return,
                    iry_cdi_investment_return_1y,
                    iry_cdi_investment_return_2y,
                    iry_cdi_investment_return_3y,
                    iry_cdi_accumulated_investment_return,
                    iry_bovespa_investment_return,
                    iry_bovespa_investment_return_1y,
                    iry_bovespa_investment_return_2y,
                    iry_bovespa_investment_return_3y,
                    iry_bovespa_accumulated_investment_return,
                    iry_bovespa_sharpe,
                    iry_bovespa_sharpe_1y,
                    iry_bovespa_sharpe_2y,
                    iry_bovespa_sharpe_3y,
                    iry_bovespa_accumulated_sharpe,
                    iry_bovespa_consistency_1y,
                    iry_bovespa_consistency_2y,
                    iry_bovespa_consistency_3y
                }
            });
        }
    };

    return stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: function (chunk, e, callback) {
            try {
                const quotize = (quotaField, quotaValor) => chunks.length == 0 ? chunk[quotaField] = 1 : chunk[quotaField] = (chunks[chunks.length - 1][quotaField] * (1 + (chunks[chunks.length - 1][quotaValor] / 100)));
                const fillNull = (field) => chunk[field] = chunk[field] == null ? (chunks.length == 0 ? 0 : chunks[chunks.length - 1][field]) : chunk[field];

                // Work some fields
                quotize('cdi_quota', 'cdi_valor');
                fillNull('bovespa_valor');

                chunks.push(chunk);
                if (last252Chunks.length >= 252) last252Chunks.shift();
                last252Chunks.push(chunk);
                if (last504Chunks.length >= 504) last504Chunks.shift();
                last504Chunks.push(chunk);
                if (last756Chunks.length >= 756) last756Chunks.shift();
                last756Chunks.push(chunk);

                const month = getMonthHash(chunk.dt_comptc);
                const year = getYearHash(chunk.dt_comptc);
                if (!chunksByDate[year]) chunksByDate[year] = {};
                if (!chunksByDate[year][month]) chunksByDate[year][month] = [];
                chunksByDate[year][month].push(chunk);

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
                console.error(ex.stack);
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
                console.error(ex.stack);
                callback(ex);
            }
        }
    });
};

module.exports = createCalculatorStream;