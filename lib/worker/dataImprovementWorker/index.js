const Db = require('../../util/db');
const promisePipe = require('promisepipe');
const stream = require('stream');
const prettyMs = require('pretty-ms');
const convertHrtime = require('convert-hrtime');
const uuidv1 = require('uuid/v1');

const Worker = require('../worker');
const UI = require('../../util/ui');
const CONFIG = require('../../config');
const createInsertPromiseStream = require('../../stream/createInsertPromiseStream');
const createAccumulatorStream = require('../../stream/createAccumulatorStream');
const createWaitForPromisesStream = require('../../stream/createWaitForPromisesStream');

const createTotalProgressInfo = () => {
    return (progress) => `DataImprovementWorker: Improving data of ${progress.total} funds: [${'▇'.repeat(progress.percentage / 2) + '-'.repeat(100 / 2 - progress.percentage / 2)}] ${progress.percentage.toFixed(2)}% - ${prettyMs(progress.elapsed)} - speed: ${progress.speed.toFixed(2)}r/s - eta: ${Number.isFinite(progress.eta) ? prettyMs(progress.eta) : 'Unknown'}`;
};

const createTotalFinishInfo = () => {
    return (progress) => `DataImprovementWorker: Improving data took ${prettyMs(progress.elapsed)} at ${progress.speed.toFixed(2)}r/s`;
};

class DataImprovementWorker extends Worker {
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

                    ui.start('DataImprovementWorkerQuery', 'DataImprovementWorker: Querying funds to improve', null, progress => `DataImprovementWorker: Querying funds to improve took ${prettyMs(progress.elapsed)}`, true);
                    const startData = process.hrtime();

                    const queryfundsCount = await mainClient.query('SELECT DISTINCT(cnpj_fundo), denom_social FROM inf_cadastral_fi');

                    const fundsCount = parseInt(queryfundsCount.rows.length);

                    ui.update('DataImprovementWorkerQuery', { elapsed: convertHrtime(process.hrtime(startData)).milliseconds });
                    ui.stop('DataImprovementWorkerQuery');

                    const progressState = {
                        total: fundsCount,
                        start: process.hrtime(),
                        elapsed: 0,
                        finished: 0,
                        percentage: 0,
                        eta: 0,
                        speed: 0
                    };

                    ui.start('total', 'DataImprovementWorker: Improving data', createTotalProgressInfo(fundsCount), createTotalFinishInfo());
                    ui.update('total', progressState);

                    const updateUI = () => {
                        progressState.elapsed = convertHrtime(process.hrtime(progressState.start)).milliseconds;
                        progressState.speed = progressState.finished / (progressState.elapsed / 100);
                        progressState.eta = ((progressState.elapsed * progressState.total) / progressState.finished) - progressState.elapsed;
                        progressState.percentage = (progressState.finished * 100) / progressState.total;
                        ui.update('total', progressState);
                    };

                    const updateUIProgressStream = stream.Transform({
                        objectMode: true,
                        highWaterMark: CONFIG.highWaterMark,
                        transform: (chunk, e, callback) => {
                            progressState.finished++;
                            updateUI();
                            callback(null, chunk);
                        },
                        flush: (callback) => {
                            updateUI();
                            callback();
                        }
                    });

                    const fundsStream = new stream.Readable({
                        objectMode: true,
                        highWaterMark: CONFIG.highWaterMark
                    });

                    const createDataImprovementStream = () => stream.Transform({
                        objectMode: true,
                        highWaterMark: CONFIG.highWaterMark,
                        transform: async (chunk, e, callback) => {
                            try {
                                const shortText = (text) => text
                                    .replace(/(^| )(FUNDOS? DE INVESTIMENTOS? RENDA FIXA)( |$)/ig, '$1FIRF$3')
                                    .replace(/(^| )(FUNDOS? DE INVESTIMENTOS? (?:EM |DE )?(?:COTAS|QUOTAS))( |$)/ig, '$1FIC$3')
                                    .replace(/(^| )(FUNDOS? DE INVESTIMENTOS? MULTIMERCADO)( |$)/ig, '$1FIM$3')
                                    .replace(/(^| )(FUNDOS? DE INVESTIMENTOS? (?:EM |DE )?A[CÇ][OÕ]ES)( |$)/ig, '$1FIA$3')
                                    .replace(/(^| )(FI RENDA FIXA)( |$)/ig, '$1FIRF$3')
                                    .replace(/(^| )(FI MULTIMERCADO)( |$)/ig, '$1FIM$3')
                                    .replace(/(^| )(FI (?:EM |DE )?(?:COTAS|QUOTAS))( |$)/ig, '$1FIC$3')
                                    .replace(/(^| )(FI (?:EM |DE )?A[CÇ][OÕ]ES)( |$)/ig, '$1FIA$3')
                                    .replace(/(^| )(FUNDOS? DE INVESTIMENTOS?)( |$)/ig, '$1FI$3')
                                    .replace(/(^| )(RENDA FIXA)( |$)/ig, '$1RF$3')
                                    .replace(/(^| )(CR[ÉE]DITO PRIVADO)( |$)/ig, '$1CP$3')
                                    .replace(/(^| )(-)( |$)/ig, ' ')
                                    .replace(/(^| )(INVESTIMENTOS? NO EXTERIOR)( |$)/ig, '$1IE$3')
                                    .replace(/(^| )(LONGO PRAZO)( |$)/ig, '$1LP$3')
                                    .replace(/(^| )(MULTIMERCADO FIM)( |$)/ig, '$1FIM$3');

                                const removeAccents = (text) => text
                                    .replace(/[àáâãäå]/gi, 'a')
                                    .replace(/æ/gi, 'ae')
                                    .replace(/ç/gi, 'c')
                                    .replace(/[èéêë]/gi, 'e')
                                    .replace(/[ìíîï]/gi, 'i')
                                    .replace(/ñ/gi, 'n')
                                    .replace(/[òóôõö]/gi, 'o')
                                    .replace(/œ/gi, 'oe')
                                    .replace(/[ùúûü]/gi, 'u')
                                    .replace(/[ýÿ]/gi, 'y');

                                const f_name = chunk.denom_social;
                                const f_short_name = shortText(f_name);

                                const data = {
                                    table: 'funds',
                                    primaryKey: ['f_cnpj'],
                                    fields: {
                                        f_id: uuidv1(),
                                        f_cnpj: chunk.cnpj_fundo,
                                        f_short_name,
                                        f_name,
                                        f_unaccented_short_name: removeAccents(f_short_name),
                                        f_unaccented_name: removeAccents(f_name)
                                    }
                                };
                                callback(null, data);
                            } catch (ex) {
                                console.error(ex);
                                callback(ex);
                            }
                        }
                    });

                    const dataImprovementPipe = promisePipe(
                        fundsStream,
                        updateUIProgressStream,
                        createDataImprovementStream(),
                        createAccumulatorStream(),
                        createInsertPromiseStream(db),
                        createWaitForPromisesStream()
                    );

                    queryfundsCount.rows.forEach(item => fundsStream.push(item));
                    fundsStream.push(null);

                    await dataImprovementPipe;

                    ui.stop('total');
                    ui.close();
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

module.exports = DataImprovementWorker;