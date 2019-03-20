const Db = require('../../util/db');
const prettyMs = require('pretty-ms');
const convertHrtime = require('convert-hrtime');
const stringSimilarity = require('string-similarity');
const allKeys = require('promise-results/allKeys');
const puppeteer = require('puppeteer');
const isDocker = require('is-docker');
const asyncPool = require('tiny-async-pool');

const moment = require('moment');
//const fs = require('fs');

const Worker = require('../worker');
const UI = require('../../util/ui');
const formatters = require('../../util/formatters');
const XPIFundListExtractor = require('./xpiFundListExtractor');

const createDataProgressInfo = () => {
    return (progress) => `XPIFundWorker: Getting data from XPI of ${progress.total} funds: [${'▇'.repeat(progress.percentage / 2) + '-'.repeat(100 / 2 - progress.percentage / 2)}] ${progress.percentage.toFixed(2)}% - ${prettyMs(progress.elapsed)} - speed: ${progress.speed.toFixed(2)}r/s - eta: ${Number.isFinite(progress.eta) ? prettyMs(progress.eta) : 'Unknown'}`;
};

const createDataFinishInfo = () => {
    return (progress) => `XPIFundWorker: Getting data from XPI took ${prettyMs(progress.elapsed)} at ${progress.speed.toFixed(2)}r/s`;
};

const createProcessingProgressInfo = () => {
    return (progress) => `XPIFundWorker: Processing data of ${progress.total} funds: [${'▇'.repeat(progress.percentage / 2) + '-'.repeat(100 / 2 - progress.percentage / 2)}] ${progress.percentage.toFixed(2)}% - ${prettyMs(progress.elapsed)} - speed: ${progress.speed.toFixed(2)}r/s - eta: ${Number.isFinite(progress.eta) ? prettyMs(progress.eta) : 'Unknown'}`;
};

const createProcessingFinishInfo = () => {
    return (progress) => `XPIFundWorker: Processing data took ${prettyMs(progress.elapsed)} at ${progress.speed.toFixed(2)}r/s and found ${((progress.found * 100) / progress.total).toFixed(2)}%`;
};

class XPIFundWorker extends Worker {
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

                    await mainClient.query('BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');

                    ui.start('XPIFundWorkerData', 'XPIFundWorker: Getting data from XPI', createDataProgressInfo(), createDataFinishInfo());

                    const { funds, xpiFunds } = await getFundsData(mainClient, (progress) => ui.update('XPIFundWorkerData', progress));

                    ui.stop('XPIFundWorkerData');

                    ui.start('XPIFundWorkerProcessing', 'XPIFundWorker: Processing data', createProcessingProgressInfo(), createProcessingFinishInfo());

                    const rowsToUpsert = discoverFunds(funds, xpiFunds, (progress) => ui.update('XPIFundWorkerProcessing', progress));

                    ui.stop('XPIFundWorkerProcessing');

                    ui.start('XPIFundWorkerUpdate', 'XPIFundWorker: Updating database', null, progress => `XPIFundWorker: Updating database took ${prettyMs(progress.elapsed)}`, true);
                    const startUpdate = process.hrtime();

                    let newQuery = db.createUpsertQuery({
                        table: 'xpi_funds',
                        primaryKey: 'xf_id',
                        values: rowsToUpsert
                    });

                    await mainClient.query({
                        text: newQuery,
                        rowMode: 'array'
                    });

                    ui.update('XPIFundWorkerUpdate', { elapsed: convertHrtime(process.hrtime(startUpdate)).milliseconds });
                    ui.stop('XPIFundWorkerUpdate');

                    await mainClient.query('COMMIT');

                    ui.close();
                } catch (ex) {
                    await mainClient.query('ROLLBACK');
                    throw ex;
                } finally {
                    mainClient.release();
                }
            } catch (ex) {
                throw ex;
            } finally {
                await db.takeOffline();
            }
        } catch (ex) {
            console.error(ex.stack);
        }
    }
}

const getFundsData = async (pool, progressCallback) => {
    let browser = null;
    if (isDocker()) {
        browser = await puppeteer.launch({
            headless: true,
            executablePath: '/usr/bin/chromium-browser',
            args: [
                '--lang=en-US',
                '--disable-dev-shm-usage'
            ]
        });
    } else {
        browser = await puppeteer.launch({
            headless: true,
            args: [
                '--lang=en-US'
            ]
        });
    }

    let xpiFunds = null;

    try {
        const extractor = new XPIFundListExtractor(browser);
        const xpiFundList = await extractor.extract();

        const progressState = {
            total: xpiFundList.rows.length,
            start: process.hrtime(),
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

        reportProgress(progressState);

        // return allKeys({
        //     funds: pool.query('SELECT f_cnpj, f_short_name FROM funds'),
        //     xpiFunds: JSON.parse(fs.readFileSync('./temp.json'))
        // });

        const xpiFundObjectList = xpiFundList.rows.map(row => {
            return {
                'formalRisk': row[0],
                'morningstar': row[1],
                'name': row[2],
                'initialInvestment': row[3],
                'rescueQuota': row[5],
                'id': row[9],
                'state': row[10],
                'admFee': row[16],
                'performanceFee': row[17],
                'benchmark': row[18],
                'type': row[19],
                'cnpj': row[20]
            };
        });

        xpiFunds = await asyncPool(4, xpiFundObjectList, async item => {
            const page = await browser.newPage();
            await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36');
            await page.goto(`https://institucional.xpi.com.br/investimentos/fundos-de-investimento/detalhes-de-fundos-de-investimento.aspx?F=${item.id}`, { waitUntil: ['load', 'networkidle2'] });

            let cnpj = await page.evaluate(() => {
                let item = document.querySelector('#lnkCaracteristica') || document.querySelector('#lnkRegulamento');
                return item ? (item.href.match(/[0-9]+/g) ? item.href.match(/[0-9]+/g)[0] : '') : '';
            });

            let extraData = await page.evaluate(() => {
                const getTextContent = (selector) => {
                    const item = document.querySelector(selector);
                    return item && item.textContent && item.textContent.trim();
                };
                const initialInvestment = getTextContent('#informacoes > table > tbody > tr:nth-child(1) > td:nth-child(2)');
                const minimalMovement = getTextContent('#informacoes > table > tbody > tr:nth-child(2) > td:nth-child(2)');
                const minimalAmountToStay = getTextContent('#informacoes > table > tbody > tr:nth-child(3) > td:nth-child(2)');
                const admFee = getTextContent('#informacoes > table > tbody > tr:nth-child(4) > td:nth-child(2)');
                const maxAdmFee = getTextContent('#informacoes > table > tbody > tr:nth-child(5) > td:nth-child(2)');
                const performanceFee = getTextContent('#informacoes > table > tbody > tr:nth-child(6) > td:nth-child(2)');
                const taxText = getTextContent('#informacoes > table > tbody > tr:nth-child(7) > td:nth-child(2)');
                const iofText = getTextContent('#informacoes > table > tbody > tr:nth-child(8) > td:nth-child(2)');
                const investmentQuota = getTextContent('#informacoes > table > tbody > tr:nth-child(9) > td:nth-child(2)');
                const rescueQuota = getTextContent('#informacoes > table > tbody > tr:nth-child(10) > td:nth-child(2)');
                const rescueFinancialSettlement = getTextContent('#informacoes > table > tbody > tr:nth-child(11) > td:nth-child(2)');
                const investmentRescueTime = getTextContent('#informacoes > table > tbody > tr:nth-child(12) > td:nth-child(2)');
                const anbimaRating = getTextContent('#informacoes > table > tbody > tr:nth-child(14) > td:nth-child(2)');
                const anbimaCode = getTextContent('#informacoes > table > tbody > tr:nth-child(15) > td:nth-child(2)');
                const custody = getTextContent('#informacoes > table > tbody > tr:nth-child(16) > td:nth-child(2)');
                const auditing = getTextContent('#informacoes > table > tbody > tr:nth-child(17) > td:nth-child(2)');
                const manager = getTextContent('#informacoes > table > tbody > tr:nth-child(18) > td:nth-child(2)');
                const administrator = getTextContent('#informacoes > table > tbody > tr:nth-child(19) > td:nth-child(2)');
                const startDate = getTextContent('#dtInicial');
                const netEquity = getTextContent('#lblVrPl');
                const netEquity1Y = getTextContent('#lblVrPl12');

                return {                    
                    initialInvestment,
                    minimalMovement,
                    minimalAmountToStay,
                    admFee,
                    maxAdmFee,
                    performanceFee,
                    taxText,
                    iofText,
                    investmentQuota,
                    rescueQuota,
                    rescueFinancialSettlement,
                    investmentRescueTime,
                    anbimaRating,
                    anbimaCode,
                    custody,
                    auditing,
                    manager,
                    administrator,
                    startDate,
                    netEquity,
                    netEquity1Y
                };
            });

            Object.assign(item, extraData);

            if (cnpj.length == 14) item.cnpj = cnpj;

            progressState.finished++;
            reportProgress(progressState);

            await page.close();

            return item;
        });
    } catch (ex) {
        throw ex;
    } finally {
        await browser.close();
    }

    //fs.writeFileSync('./temp.json', JSON.stringify(xpiFunds));

    return allKeys({
        funds: pool.query('SELECT f_cnpj, f_short_name FROM funds'),
        xpiFunds: xpiFunds
    });
};

const discoverFunds = (funds, xpiFunds, progressCallback) => {
    const progressState = {
        total: xpiFunds.length,
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
            .replace(/(^| )(strategiesficfi)( |$)/ig, '$1strategies fic fi$3')
            .replace(/(^| )(investimento exterior)( |$)/ig, '$1ie$3')
            .replace(/(^| )(longo prazo)( |$)/ig, '$1lp$3')
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
            .replace(/(^| )(fi renda fixa)( |$)/ig, '$1firf$3')
            .replace(/(^| )(renda fixa)( |$)/ig, '$1rf$3')
            .replace(/(^| )(multimercado fim)( |$)/ig, '$1fim$3');
    };

    const avaliableFundsName = [];
    const avaliableFundsNameIndex = [];

    funds.rows.map((row, index) => {
        const normalizedName = normalize(row.f_short_name);
        avaliableFundsName.push(normalizedName);
        avaliableFundsNameIndex[normalizedName] = index;
    });

    let rowsToUpsert = [];

    xpiFunds.forEach(item => {
        const row = {
            xf_id: formatters.parseInt(item.id),
            xf_cnpj: null,
            xf_date: (new Date()).toISOString(),
            xf_formal_risk: formatters.parseInt(item.formalRisk),
            xf_morningstar: formatters.parseInt(item.morningstar),
            xf_name: item.name.replace(/'/g, ''),
            xf_initial_investment: formatters.cleanBRMoney(item.initialInvestment) / 100,
            xf_minimal_movement: formatters.cleanBRMoney(item.minimalMovement) / 100,
            xf_minimal_amount_to_stay: formatters.cleanBRMoney(item.minimalAmountToStay) / 100,
            xf_investment_quota: formatters.removeRelativeBRDate(item.investmentQuota),
            xf_rescue_quota: formatters.removeRelativeBRDate(item.rescueQuota),
            xf_rescue_financial_settlement: formatters.removeRelativeBRDate(item.rescueFinancialSettlement),
            xf_investment_rescue_time: item.investmentRescueTime,
            xf_anbima_rating: item.anbimaRating,
            xf_anbima_code: item.anbimaCode,
            xf_custody: item.custody,
            xf_auditing: item.auditing,
            xf_manager: item.manager,
            xf_administrator: item.administrator,
            xf_startDate: moment.utc(item.startDate, 'DD/MM/YYYY'),
            xf_net_equity: formatters.cleanBRMoney(item.netEquity) / 100,
            xf_net_equity_1y: formatters.cleanBRMoney(item.netEquity1Y) / 100,
            xf_state: item.state == '' ? 0 : 1,
            xf_adm_fee: formatters.cleanBRMoney(item.admFee) / 10000,
            xf_max_adm_fee: formatters.cleanBRMoney(item.maxAdmFee) / 10000,
            xf_perf_fee: formatters.cleanBRMoney(item.performanceFee) / 10000,
            xf_benchmark: item.benchmark,
            xf_type: item.type,
            xf_tax_text: item.taxText,
            xf_iof_text: item.iofText
        };

        if (row.xf_cnpj == null) {
            const normalizedProduct = normalize(item.name);
            const bestMatch = stringSimilarity.findBestMatch(normalizedProduct, avaliableFundsName).bestMatch;
            if (bestMatch.rating >= 0.80) {
                progressState.found++;
                row.xf_cnpj = funds.rows[avaliableFundsNameIndex[bestMatch.target]].f_cnpj;
            } else {
                console.log(`Not found: (${bestMatch.rating.toFixed(2)}): ${normalizedProduct} = ${bestMatch.target}\n`);
            }
        } else progressState.found++;

        rowsToUpsert.push(row);

        progressState.finished++;
        reportProgress();
    });
    return rowsToUpsert;
};

module.exports = XPIFundWorker;