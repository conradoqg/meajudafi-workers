const puppeteer = require('puppeteer');
const promiseLimit = require('promise-limit');
const uuidv1 = require('uuid/v1');

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
                    const browser = await puppeteer.launch({
                        headless: true,
                        executablePath: '/usr/bin/chromium-browser',
                        args: [
                            '--lang=en-US',
                            '--disable-dev-shm-usage'
                        ]
                    });

                    try {
                        await mainClient.query('BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');

                        const extractor = new XPIFundListExtractor(browser);
                        const xpiFundList = await extractor.extract();

                        const xpiFundObjectList = xpiFundList.rows.map(row => {
                            return {
                                'FORMAL_RISK': row[0],
                                'MORNINGSTAR': row[1],
                                'NAME': row[2],
                                'INITIAL_INVESTMENT': row[3],
                                'REDEMPTION_DELAY_IN_DAYS': row[5],
                                'XPI_ID': row[9],
                                'STATE': row[10],
                                'ADM_FEE': row[16],
                                'PERF_FEE': row[17],
                                'BENCHMARK': row[18],
                                'TYPE': row[19],
                                'CNPJ': row[20]
                            };
                        });

                        const xpi_funds_metadata = {
                            table: 'xpi_funds',
                            formatMap: {
                                'FORMAL_RISK': formatters.parseInt,
                                'MORNINGSTAR': formatters.parseInt,
                                'INITIAL_INVESTMENT': (value) => formatters.cleanBRMoney(value) / 100,
                                'REDEMPTION_DELAY_IN_DAYS': formatters.removeRelativeBRDate,
                                'STATE': (value) => value == '' ? 0 : 1,
                                'ADM_FEE': (value) => formatters.cleanBRMoney(value) / 10000,
                                'PERF_FEE': (value) => formatters.cleanBRMoney(value) / 10000
                            }
                        };

                        const formattedXPIFundList = xpiFundObjectList.map(item => {
                            item.id = uuidv1();
                            Object.keys(xpi_funds_metadata.formatMap).map(key => {
                                item[key] = item[key] ? xpi_funds_metadata.formatMap[key](item[key]) : item[key];
                            });
                            return item;
                        });

                        const existingXPIFunds = await mainClient.query('SELECT CNPJ, XPI_ID FROM xpi_funds');

                        const limiter = promiseLimit(4);

                        await Promise.all(formattedXPIFundList.map(async row => limiter(async () => {
                            try {
                                const found = existingXPIFunds.rows.find(fund => fund.xpi_id == row.XPI_ID);
                                if (found) {
                                    row.CNPJ = found.cnpj;
                                } else {
                                    const xpiCNPJFundFinder = new XPICNPJFundFinder(browser);
                                    row.CNPJ = await xpiCNPJFundFinder.extract(row.XPI_ID, row.NAME);
                                }
                            } catch (ex) {
                                console.error(ex);
                            }
                            return row;
                        })));

                        const rowsToUpsert = formattedXPIFundList.filter(row => row.CNPJ != null);

                        if (rowsToUpsert.length > 0) {
                            let newQuery = db.createUpsertQuery({
                                table: xpi_funds_metadata.table,
                                primaryKey: 'CNPJ',
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