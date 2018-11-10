const fs = require('fs-extra');
const promisePipe = require('promisepipe');
const promiseLimit = require('promise-limit');
const stream = require('stream');
const progress = require('progress-stream');
const unzipStream = require('unzip-stream');
const path = require('path');
const got = require('got');
const prettyBytes = require('pretty-bytes');
const prettyMs = require('pretty-ms');
const convertHrtime = require('convert-hrtime');

const UI = require('./util/ui');
const CONFIG = require('./config');
const createDownloadStream = require('./stream/createDownloadStream');
const createFileUnzipStream = require('./stream/createFileUnzipStream');
const createFileProcessorStream = require('./stream/createFileProcessorStream');

const storeOrLoadProcessedList = (processedList) => {
    const PROCESSEDLIST_FILENAME = './db/processedList.json';
    if (processedList == null) {
        if (fs.existsSync(PROCESSEDLIST_FILENAME)) return fs.readJSONSync(PROCESSEDLIST_FILENAME);
        else return {};
    } else {
        fs.writeJSONSync(PROCESSEDLIST_FILENAME, processedList);
        return processedList;
    }
};

const fileHasChanged = (filename, eTag, processedList) => {
    const foundETag = Object.keys(processedList).find(filenameItem => filenameItem == filename);

    return typeof (foundETag) == 'undefined' || processedList[foundETag] != eTag;
};

const isDownloadNecessary = async (filename, processedList) => {
    const result = await got.head(filename);

    return { shouldDownload: result && fileHasChanged(filename, result.headers.etag, processedList), etag: result.headers.etag };
};

const createTotalProgressInfo = () => {
    return (progress) => `CVMDataProcess Overall (${progress.total}): [${'▇'.repeat(progress.percentage) + '-'.repeat(100 - progress.percentage)}] ${progress.percentage.toFixed(2)}% - ${prettyMs(progress.elapsed)} - speed: ${progress.speed.toFixed(2)}r/s - eta: ${Number.isFinite(progress.eta) ? prettyMs(progress.eta) : 'Unknown'}`;
};

const createTotalFinishInfo = () => {
    return (progress) => `CVMDataProcess Overall took ${prettyMs(progress.elapsed)} at ${progress.speed.toFixed(2)}r/s`;
};

const createStreamProgressInfo = (type) => {
    return (progress) => `${type}: [${'▇'.repeat(progress.percentage) + '-'.repeat(100 - progress.percentage)}] ${progress.percentage.toFixed(2)}% - ${prettyBytes(progress.speed)}/s - ${Number.isFinite(progress.eta) ? prettyMs(progress.eta * 1000) : 'Unknown'} remaining`;
};
const createDownloadStreamFinishInfo = (type) => {
    return (progress) => `${type}: size ${prettyBytes(progress.transferred)} - took ${prettyMs(progress.runtime * 1000)} at ${prettyBytes(progress.speed)}/s`;
};
const createTransformStreamFinishInfo = (type) => {
    return (progress) => `${type}: size ${prettyBytes(progress.transferred)} - took ${prettyMs(progress.runtime * 1000)} at ${prettyBytes(progress.speed)}/s`;
};

module.exports = async (db, dataList) => {
    const processedList = storeOrLoadProcessedList();
    const ui = new UI();
    const limit = promiseLimit(4);
    const progressState = {
        total: dataList.length,
        start: process.hrtime(),
        elapsed: 0,
        finished: 0,
        percentage: 0,
        eta: 0,
        speed: 0
    };

    ui.start('total', 'Processing', createTotalProgressInfo(dataList.length), createTotalFinishInfo());
    ui.update('total', progressState);

    await Promise.all(dataList.map((data) => limit(async () => {
        const fundListURL = data.url;

        const { shouldDownload, etag } = await isDownloadNecessary(fundListURL, processedList);

        if (shouldDownload) {
            const downloadProgressStream = progress({
                time: 500,
                length: 'auto',
                drain: true
            });
            downloadProgressStream.on('progress', (progress) => ui.update(`download-${data.url}`, progress));

            const client = await db.pool.connect();

            try {
                await client.query('BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');

                if (!data.metadata.upsertConflictField) await db.pool.query(`TRUNCATE ${data.metadata.table}`);

                const urlDownloadStream = createDownloadStream(fundListURL);

                const url = require('url');
                const parsedURL = url.parse(data.url);
                const localFilename = path.basename(parsedURL.pathname);

                try {
                    ui.start(`download-${data.url}`, 'Downloading', createStreamProgressInfo(`Downloading ${localFilename}`), createDownloadStreamFinishInfo(`Downloaded ${localFilename}`));
                    let retriesLeft = CONFIG.downloadRetries;
                    let success = false;
                    while (retriesLeft-- > 0) {
                        try {
                            await promisePipe(
                                urlDownloadStream,
                                downloadProgressStream,
                                fs.createWriteStream(localFilename)
                            );
                            success = true;
                            break;
                        } catch (ex) {
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

                        ui.start(`read-${data.url}`, 'Transforming', createStreamProgressInfo(`Transforming ${localFilename}`), createTransformStreamFinishInfo(`Transformed ${localFilename}`));
                        if (path.extname(data.url).toLowerCase() == '.zip') {
                            await promisePipe(
                                fs.createReadStream(localFilename),
                                readProgressStream,
                                unzipStream.Parse(),
                                createFileUnzipStream(createFileProcessorStream, db, data, data.url),
                                stream.Writable()
                            );
                        } else {
                            await promisePipe(
                                fs.createReadStream(localFilename),
                                readProgressStream,
                                ...createFileProcessorStream(db, data, data.url)
                            );
                        }
                        ui.stop(`read-${data.url}`);

                        processedList[fundListURL] = etag;

                        storeOrLoadProcessedList(processedList);

                        console.log('commitando')
                        await client.query('COMMIT');
                        console.log('commitado')
                    } else {
                        throw new Error(`\nUnable to download url ${data.url} after ${CONFIG.downloadRetries} retries`);
                    }
                } finally {
                    await fs.unlink(localFilename);
                }
            } catch (ex) {
                await client.query('ROLLBACK');
                console.error(ex.stack);
                throw ex;
            } finally {
                client.release();
            }
        }

        progressState.finished++;
        progressState.elapsed = convertHrtime(process.hrtime(progressState.start)).milliseconds;
        progressState.speed = progressState.finished / (progressState.elapsed / 100);
        progressState.eta = ((progressState.elapsed * progressState.total) / progressState.finished) - progressState.elapsed;
        progressState.percentage = (progressState.finished * 100) / progressState.total;

        ui.update('total', progressState);
    })));

    ui.stop('total');
    ui.close();
};
