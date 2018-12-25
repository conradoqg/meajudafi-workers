const puppeteer = require('puppeteer');
const promiseLimit = require('promise-limit');
const uuidv1 = require('uuid/v1');
const isDocker = require('is-docker');

const Worker = require('./worker');
const Db = require('./util/db');
const XPIFundListExtractor = require('./xpiFundListExtractor');
const XPICNPJFundFinder = require('./xpiCNPJFundFinder');
const formatters = require('./util/formatters');

class XPIFundProcess extends Worker {
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
                    // Setup puppeteer
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
                            headless: false,
                            args: [
                                '--lang=en-US'
                            ]
                        });
                    }

                    try {
                        await mainClient.query('BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');

                        const extractor = new XPIFundListExtractor(browser);
                        const xpiFundList = await extractor.extract();

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

                        const limiter = promiseLimit(1);

                        await Promise.all(formattedXPIFundList.map(async row => limiter(async () => {
                            try {
                                const found = existingXPIFunds.rows.find(fund => fund.xf_xpi_id == row.xf_XPI_ID);
                                if (found) {
                                    row.xf_CNPJ = found.xf_cnpj;
                                } else {
                                    const xpiCNPJFundFinder = new XPICNPJFundFinder(browser);
                                    row.xf_CNPJ = await xpiCNPJFundFinder.extract(row.xf_XPI_ID, row.xf_NAME);
                                }
                            } catch (ex) {
                                console.error(ex);
                            }
                            return row;
                        })));

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

module.exports = XPIFundProcess;