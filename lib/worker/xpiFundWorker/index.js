const puppeteer = require('puppeteer');
const promiseLimit = require('promise-limit');
const uuidv1 = require('uuid/v1');
const isDocker = require('is-docker');
const convertHrtime = require('convert-hrtime');
const prettyMs = require('pretty-ms');

const Worker = require('../worker');
const UI = require('../../util/ui');
const Db = require('../../util/db');
const XPIFundListExtractor = require('./xpiFundListExtractor');
const XPICNPJFundFinder = require('./xpiCNPJFundFinder');
const formatters = require('../../util/formatters');

const createFundFinderProgressInfo = () => {
    return (progress) => `Finding funds CNPJ on Google (${progress.total}): [${'▇'.repeat((progress.percentage / 2)) + '-'.repeat((100 - progress.percentage) / 2)}] ${progress.percentage.toFixed(2)}% - ${prettyMs(progress.elapsed)} - speed: ${progress.speed.toFixed(2)}CNPJ/s - eta: ${Number.isFinite(progress.eta) ? prettyMs(progress.eta) : 'Unknown'}`;
};

const createFundFinderFinishInfo = () => {
    return (progress) => `Found funds CNPJ on Google took ${prettyMs(progress.elapsed)} at ${progress.speed.toFixed(2)}CNPJ/s`;
};

class XPIFundWorker extends Worker {
    constructor() {
        super();
    }

    async work() {
        try {

            const ui = new UI();

            const db = new Db();

            await db.takeOnline();

            try {
                const mainClient = await db.pool.connect();

                try {
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

                    try {
                        await mainClient.query('BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');

                        ui.start('XPIFundListExtractor', 'Extracting data from XPI', null, progress => `Extracted data from XPI took ${prettyMs(progress.elapsed)}`, true);
                        const start = process.hrtime();

                        const extractor = new XPIFundListExtractor(browser);
                        const xpiFundList = await extractor.extract();

                        ui.update('XPIFundListExtractor', { elapsed: convertHrtime(process.hrtime(start)).milliseconds });
                        ui.stop('XPIFundListExtractor');

                        const xpiFundObjectList = xpiFundList.rows.map(row => {
                            return {
                                'xf_FORMAL_RISK': row[0],
                                'xf_MORNINGSTAR': row[1],
                                'xf_NAME': row[2],
                                'xf_INITIAL_INVESTMENT': row[3],
                                'xf_REDEMPTION_DELAY_IN_DAYS': row[5],
                                'xf_XPI_ID': row[9],
                                'xf_STATE': row[10],
                                'xf_ADM_FEE': row[16],
                                'xf_PERF_FEE': row[17],
                                'xf_BENCHMARK': row[18],
                                'xf_TYPE': row[19],
                                'xf_CNPJ': row[20]
                            };
                        });

                        const xpi_funds_metadata = {
                            table: 'xpi_funds',
                            formatMap: {
                                'xf_FORMAL_RISK': formatters.parseInt,
                                'xf_MORNINGSTAR': formatters.parseInt,
                                'xf_INITIAL_INVESTMENT': (value) => formatters.cleanBRMoney(value) / 100,
                                'xf_REDEMPTION_DELAY_IN_DAYS': formatters.removeRelativeBRDate,
                                'xf_STATE': (value) => value == '' ? 0 : 1,
                                'xf_ADM_FEE': (value) => formatters.cleanBRMoney(value) / 10000,
                                'xf_PERF_FEE': (value) => formatters.cleanBRMoney(value) / 10000
                            }
                        };

                        const formattedXPIFundList = xpiFundObjectList.map(item => {
                            item.xf_id = uuidv1();
                            Object.keys(xpi_funds_metadata.formatMap).map(key => {
                                item[key] = item[key] ? xpi_funds_metadata.formatMap[key](item[key]) : item[key];
                            });
                            return item;
                        });

                        const existingXPIFunds = await mainClient.query('SELECT xf_CNPJ, xf_XPI_ID FROM xpi_funds');

                        const unknownFunds = [];

                        formattedXPIFundList.map(row => {
                            const found = existingXPIFunds.rows.find(fund => fund.xf_xpi_id == row.xf_XPI_ID);

                            if (found) {
                                row.xf_CNPJ = found.xf_cnpj;
                            } else {
                                unknownFunds.push(row);
                            }
                        });

                        const limiter = promiseLimit(1);

                        const progressState = {
                            total: unknownFunds.length,
                            view: '',
                            start: process.hrtime(),
                            elapsed: 0,
                            finished: 0,
                            percentage: 0,
                            eta: 0,
                            speed: 0
                        };

                        ui.start('XPICNPJFundFinder', 'Finding funds CNPJ on Google', createFundFinderProgressInfo(), createFundFinderFinishInfo());

                        await Promise.all(unknownFunds.map(async row => limiter(async () => {
                            try {                            
                                const xpiCNPJFundFinder = new XPICNPJFundFinder(browser);
                                row.xf_CNPJ = await xpiCNPJFundFinder.extract(row.xf_XPI_ID, row.xf_NAME);                                
                            } catch (ex) {
                                console.error(ex);
                            }

                            progressState.finished++;

                            progressState.elapsed = convertHrtime(process.hrtime(progressState.start)).milliseconds;
                            progressState.speed = progressState.finished / (progressState.elapsed / 100);
                            progressState.eta = ((progressState.elapsed * progressState.total) / progressState.finished) - progressState.elapsed;
                            progressState.percentage = (progressState.finished * 100) / progressState.total;
                            ui.update('XPICNPJFundFinder', progressState);
                            return Promise.resolve();
                        })));

                        ui.stop('XPICNPJFundFinder');

                        // Ignore nulls
                        let rowsToUpsert = formattedXPIFundList.filter(row => row.xf_CNPJ != null);
                        // Ignore duplicates
                        rowsToUpsert = rowsToUpsert.filter((row, index, self) => index === self.findIndex((t) => t.xf_CNPJ === row.xf_CNPJ));

                        if (rowsToUpsert.length > 0) {

                            let newQuery = db.createUpsertQuery({
                                table: xpi_funds_metadata.table,
                                primaryKey: 'xf_CNPJ',
                                values: rowsToUpsert
                            });

                            await mainClient.query({
                                text: newQuery,
                                rowMode: 'array'
                            });
                        }
                        await mainClient.query('COMMIT');
                    } catch (ex) {
                        await mainClient.query('ROLLBACK');
                        throw ex;
                    } finally {
                        await browser.close();
                    }
                } catch (ex) {
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

module.exports = XPIFundWorker;