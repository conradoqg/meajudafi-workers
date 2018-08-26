const csv = require('fast-csv');
const fs = require('fs-extra');
const iconv = require('iconv-lite');
const promisePipe = require('promisepipe');
const promiseLimit = require('promise-limit');
const uuidv1 = require('uuid/v1');
const stream = require('stream');
const progress = require('progress-stream');
const unzipStream = require('unzip-stream');
const path = require('path');
const got = require('got');
const prettyBytes = require('pretty-bytes');
const prettyMs = require('pretty-ms');
const UI = require('./ui');
const convertHrtime = require('convert-hrtime');

const CONFIG = {
    batchSize: 500,
    highWaterMark: 10,
    downloadRetries: 3
};

const QUOTE = /'/g;

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

const createDownloadStream = (url) => {
    return got.stream(url);
};

const createEncodingTransformStream = () => {
    return [iconv.decodeStream('WINDOWS-1252'), iconv.encodeStream('UTF-8')];
};

const createCSVParserAndTransformerStream = (formatMap) => {
    let header = null;

    return csv({ delimiter: ';', includeEndRowDelimiter: true })
        .transform((data) => {
            try {
                if (header == null) header = data;
                else {
                    try {
                        const preparedData = {};
                        data.map((item, index) => {
                            if (formatMap[header[index]]) preparedData[header[index]] = formatMap[header[index]](item);
                            else preparedData[header[index]] = item ? item.replace(QUOTE, '') : null;
                        });

                        return preparedData;
                    } catch (ex) {
                        console.error(ex.stack);
                        throw ex;
                    }
                }
            } catch (ex) {
                console.error(ex.stack);
            }
        });
};

const createBatchTransformStream = (table, primaryKey, batchSize = CONFIG.batchSize) => {
    let values = {};

    return new stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: (chunk, e, callback) => {
            try {
                chunk.id = uuidv1();
                const hash = Array.isArray(primaryKey) ? primaryKey.map(key => chunk[key]).join('') : chunk.id;
                values[hash] = chunk;

                if (Object.keys(values).length >= batchSize) {
                    const valuesCopy = values;
                    values = {};
                    callback(null, {
                        table,
                        primaryKey,
                        values: Object.values(valuesCopy)
                    });
                } else {
                    callback(null, null);
                }
            } catch (ex) {
                console.error(ex.stack);
            }
        },
        flush: (callback) => {
            try {
                if (Object.keys(values).length > 0) {
                    const valuesCopy = values;
                    values = {};
                    callback(null, {
                        table,
                        primaryKey,
                        values: Object.values(valuesCopy)
                    });
                } else callback();
            } catch (ex) {
                console.error(ex.stack);
            }
        }
    });
};

const createInsertPromiseStream = (pool) => {
    return new stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: async (chunk, e, callback) => {
            try {
                const client = await pool.connect();
                try {
                    const values = chunk.values.map(values => `(${Object.values(values).map(value => (value != null ? `'${value}'` : 'null')).join(', ')})`).join(',');
                    const fields = Object.keys(chunk.values[0]).join(', ');
                    const onConflict = (Array.isArray(chunk.primaryKey) ? chunk.primaryKey.join(', ') : chunk.primaryKey);
                    const updateFields = Object.keys(chunk.values[1]).slice(0).map(value => `${value} = excluded.${value}`).join(', ');

                    let newQuery = null;
                    if (chunk.primaryKey) newQuery = `INSERT INTO ${chunk.table} (${fields}) VALUES ${values} ON CONFLICT (${onConflict}) DO UPDATE SET ${updateFields}`;
                    else newQuery = `INSERT INTO ${chunk.table} (${fields}) VALUES ${values}`;

                    const queryPromise = client.query({
                        text: newQuery,
                        rowMode: 'array'
                    });
                    callback(null, queryPromise);
                    await queryPromise;
                } catch (ex) {
                    console.error(ex.stack);
                    callback(ex);
                } finally {
                    client.release();
                }
            } catch (ex) {
                console.error(ex.stack);
                callback(ex);
            }
        }
    });
};

const createWaitForPromisesStream = () => {
    return new stream.Writable({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        write: async (chunk, e, callback) => {
            try {
                await chunk;
                callback(null, chunk);
            } catch (ex) {
                console.error(ex.stack);
                callback(ex);
            }
        }
    });
};

const createFileUnzipStream = (createFileProcessorStream, db, data) => {
    return new stream.Transform({
        objectMode: true,
        transform: async (entry, e, callback) => {
            try {
                const filePath = entry.path;
                const type = entry.type;
                if (type == 'File' && path.extname(filePath).toLowerCase() == '.csv') {
                    try {
                        await promisePipe(
                            entry,
                            ...createFileProcessorStream(db, data, filePath)
                        );
                        callback();
                    } catch (ex) {
                        entry.autodrain();
                        console.error(ex.stack);
                        callback(ex);
                    }
                } else {
                    entry.autodrain();
                    callback();
                }
            } catch (ex) {
                console.error(ex.stack);
            }
        }
    });
};

const createFileProcessorStream = (db, data) => {
    const encodingTransformStream = createEncodingTransformStream();
    const csvParserStream = createCSVParserAndTransformerStream(data.metadata.formatMap);
    const batchTransformStream = createBatchTransformStream(data.metadata.table, data.metadata.upsertConflictField);
    const createInsertPromisesStream = createInsertPromiseStream(db.pool);
    const waitForPromisesStream = createWaitForPromisesStream();
    return [
        ...encodingTransformStream,
        csvParserStream,
        batchTransformStream,
        createInsertPromisesStream,
        waitForPromisesStream
    ];
};

const createTotalProgressInfo = () => {
    return (progress) => `Overall (${progress.total} files): [${'▇'.repeat(progress.percentage) + '-'.repeat(100 - progress.percentage)}] ${progress.percentage.toFixed(2)}%`;
};

const createTotalFinishInfo = () => {
    return (progress) => `Overall took ${prettyMs(progress.elapsed)}`;
};

const createStreamProgressInfo = (type) => {
    return (progress) => `${type}: [${'▇'.repeat(progress.percentage) + '-'.repeat(100 - progress.percentage)}] ${progress.percentage.toFixed(2)}% - ${prettyBytes(progress.speed)}/s - ${prettyMs(progress.eta * 1000)} remaining`;
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
        percentage: 0
    };

    ui.start('total', createTotalProgressInfo(dataList.length), createTotalFinishInfo());
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
                    ui.start(`download-${data.url}`, createStreamProgressInfo(`Downloading ${localFilename}`), createDownloadStreamFinishInfo(`Downloaded ${localFilename}`));
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

                        ui.start(`read-${data.url}`, createStreamProgressInfo(`Transforming ${localFilename}`), createTransformStreamFinishInfo(`Transformed ${localFilename}`));
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

                        await client.query('COMMIT');
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
        progressState.percentage = (progressState.finished * 100) / progressState.total;
        ui.update('total', progressState);
    })));

    ui.stop('total');
    ui.close();
};