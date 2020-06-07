const Db = require('../util/db');

const fs = require('fs-extra');
const promisePipe = require('promisepipe');
const stream = require('stream');
const progress = require('progress-stream');
const unzipStream = require('unzip-stream');
const path = require('path');
const got = require('got');
const prettyBytes = require('pretty-bytes');
const prettyMs = require('pretty-ms');
const convertHrtime = require('convert-hrtime');
const crypto = require('crypto');

const UI = require('../util/ui');
const CONFIG = require('../config');
const createDownloadStream = require('../stream/createDownloadStream');
const createFileUnzipStream = require('../stream/createFileUnzipStream');
const createFileProcessorStream = require('../stream/createFileProcessorStream');

const createTotalProgressInfo = () => {
    return (progress) => `DataProcessor: Getting data of ${progress.total} files: [${'▇'.repeat(progress.percentage / 2) + '-'.repeat(100 / 2 - progress.percentage / 2)}] ${progress.percentage.toFixed(2)}% - ${prettyMs(progress.elapsed)} - speed: ${progress.speed.toFixed(2)}r/s - eta: ${Number.isFinite(progress.eta) ? prettyMs(progress.eta) : 'Unknown'}`;
};

const createTotalFinishInfo = () => {
    return (progress) => `DataProcessor: Getting data took ${prettyMs(progress.elapsed)} at ${progress.speed.toFixed(2)}r/s`;
};

const createStreamProgressInfo = (type) => {
    return (progress) => `DataProcessor: ${type}: [${'▇'.repeat(progress.percentage / 2) + '-'.repeat(100 / 2 - progress.percentage / 2)}] ${progress.percentage.toFixed(2)}% - ${prettyBytes(progress.speed)}/s - ${Number.isFinite(progress.eta) ? prettyMs(progress.eta * 1000) : 'Unknown'} remaining`;
};
const createStreamFinishInfo = (type) => {
    return (progress) => `DataProcessor: ${type} took ${prettyMs(progress.runtime * 1000)} at ${prettyBytes(progress.speed)}/s with total size of ${prettyBytes(progress.transferred)}`;
};

module.exports = async (db, createDataParser, dataList) => {
    const ui = new UI();
    const progressState = {
        total: dataList.length,
        start: process.hrtime(),
        elapsed: 0,
        finished: 0,
        percentage: 0,
        eta: 0,
        speed: 0
    };

    ui.start('total', 'DataProcessor: Getting data', createTotalProgressInfo(dataList.length), createTotalFinishInfo());
    ui.update('total', progressState);

    for (const data of dataList) {
        const fundListURL = data.url;

        const client = await db.pool.connect();

        try {
            let shouldDownload = false;
            let etag = null;

            try {
                const result = await got.head(fundListURL);
                etag = result.statusCode && result.statusCode == 200 && result.headers.etag ? result.headers.etag : null;

                const foundProcessedURLResult = await client.query(`SELECT etag from processed_url where url='${fundListURL}'`);
                if (foundProcessedURLResult.rowCount > 0) shouldDownload = foundProcessedURLResult.rows[0].etag != etag;
                else shouldDownload = true;

            } catch (ex) {
                if (ex.statusCode && ex.statusCode == 404) continue;
                throw new Error(`Could not download ${fundListURL}, Code: ${ex.statusCode}`);
            }

            if (shouldDownload) {
                await client.query('BEGIN TRANSACTION');

                if (!data.metadata.upsertConflictField) await client.query(`DELETE FROM ${data.metadata.table}`);

                fs.ensureDirSync('./tmp/');

                const localFilename = './tmp/' + crypto.createHash('sha1').update(fundListURL).digest('hex');

                try {
                    ui.start(`download-${data.url}`, 'DataProcessor: Downloading', createStreamProgressInfo(`Downloading ${data.name}`), createStreamFinishInfo(`Downloading ${data.name}`));
                    let retriesLeft = CONFIG.downloadRetries;
                    let success = false;
                    while (retriesLeft-- > 0) {
                        const urlDownloadStream = createDownloadStream(fundListURL);
                        const downloadProgressStream = progress({
                            time: 500,
                            length: 'auto',
                            drain: true
                        });
                        downloadProgressStream.on('progress', (progress) => ui.update(`download-${data.url}`, progress));
                        try {
                            await promisePipe(
                                urlDownloadStream,
                                downloadProgressStream,
                                fs.createWriteStream(localFilename)
                            );
                            success = true;
                            break;
                        } catch (ex) {
                            console.error('Error: dataProcessor: download');
                            console.error(ex.stack);
                        }
                    }
                    ui.stop(`download-${data.url}`);

                    if (success) {
                        const stat = fs.statSync(localFilename);

                        const readProgressStream = progress({
                            time: 500,
                            length: stat.size
                        });
                        readProgressStream.on('progress', (progress) => ui.update(`read-${data.url}`, progress));

                        ui.start(`read-${data.url}`, 'DataProcessor: Transforming', createStreamProgressInfo(`Transforming ${data.name}`), createStreamFinishInfo(`Transforming ${data.name}`));
                        if (path.extname(data.url).toLowerCase() == '.zip') {
                            await promisePipe(
                                fs.createReadStream(localFilename),
                                readProgressStream,
                                unzipStream.Parse(),
                                createFileUnzipStream(createFileProcessorStream, createDataParser, db, data),
                                stream.Writable()
                            );
                        } else {
                            await promisePipe(
                                fs.createReadStream(localFilename),
                                readProgressStream,
                                ...createFileProcessorStream(db, createDataParser, data)
                            );
                        }
                        ui.stop(`read-${data.url}`);

                        const processedURLUpsertQuery = Db.createUpsertQuery({
                            table: 'processed_url',
                            primaryKey: ['url'],
                            values: [{
                                'url': fundListURL,
                                'etag': etag
                            }]
                        });

                        await client.query(processedURLUpsertQuery);

                        await client.query('COMMIT');
                    } else {
                        throw new Error(`\nUnable to download url ${data.url} after ${CONFIG.downloadRetries} retries`);
                    }
                } finally {
                    await fs.unlink(localFilename);
                }
            }
        } catch (ex) {
            console.error('Error: dataProcessor: 1');
            console.error(ex.stack);
            if (!Db.isConnectivityError(ex)) {
                console.error('Error: dataProcessor: 2');
                await client.query('ROLLBACK');
            }
            throw ex;
        } finally {
            await client.release();
        }


        progressState.finished++;
        progressState.elapsed = convertHrtime(process.hrtime(progressState.start)).milliseconds;
        progressState.speed = progressState.finished / (progressState.elapsed / 100);
        progressState.eta = ((progressState.elapsed * progressState.total) / progressState.finished) - progressState.elapsed;
        progressState.percentage = (progressState.finished * 100) / progressState.total;

        ui.update('total', progressState);
    }

    ui.stop('total');
    ui.close();
};