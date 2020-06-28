const promisePipe = require('promisepipe');
const stream = require('stream');
const uuidv1 = require('uuid/v1');

const TransactionedWorker = require('../transactionedWorker');
const CONFIG = require('../../util/config');
const createInsertPromiseStream = require('../../stream/createInsertPromiseStream');
const createAccumulatorStream = require('../../stream/createAccumulatorStream');

class DataImprovementWorker extends TransactionedWorker {
    id = 'DataImprovementWorker'

    constructor() {
        super();
    }

    async getData(mainClient, progress) {
        progress.start();
        const query = 'SELECT DISTINCT(cnpj_fundo), denom_social FROM inf_cadastral_fi';

        try {
            return await mainClient.query(query);
        } catch (ex) {
            throw new Error(`Query "${query}" failed`, ex);
        }
    }

    async improve(mainClient, progress, funds) {
        const fundsCount = parseInt(funds.rows.length);
        progress.start(fundsCount);

        const updateUIProgressStream = stream.Transform({
            objectMode: true,
            highWaterMark: CONFIG.highWaterMark,
            transform: (chunk, e, callback) => {
                progress.step();
                callback(null, chunk);
            },
            flush: (callback) => {
                callback();
            }
        });

        const fundsStream = new stream.Readable({
            objectMode: true,
            highWaterMark: CONFIG.highWaterMark
        });

        const createDataImprovementStream = () => new stream.Transform({
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
                    callback(ex);
                }
            }
        });

        const dataImprovementPipe = promisePipe(
            fundsStream,
            updateUIProgressStream,
            createDataImprovementStream(),
            createAccumulatorStream(),
            createInsertPromiseStream(mainClient)
        );

        funds.rows.forEach(item => fundsStream.push(item));
        fundsStream.push(null);

        try {
            await dataImprovementPipe;
        } catch (ex) {
            throw ex.originalError;
        }
    }
}

module.exports = DataImprovementWorker;