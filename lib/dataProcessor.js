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

const CONFIG = {
    batchSize: 500,
    highWaterMark: 50
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

    return result && fileHasChanged(filename, result.headers.etag, processedList);
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
        });
};

const createBatchTransformStream = (table, primaryKey, batchSize = CONFIG.batchSize) => {
    let values = [];

    return new stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: (chunk, e, callback) => {
            try {
                values.push(Object.assign({ id: uuidv1() }, chunk));

                if (values.length >= batchSize) {
                    const valuesCopy = values;
                    values = [];
                    callback(null, {
                        table,
                        primaryKey,
                        values: valuesCopy
                    });
                } else {
                    callback(null, null);
                }
            } catch (ex) {
                console.error(ex.stack);
            }
        },
        flush: (callback) => {
            const valuesCopy = values;
            values = [];
            callback(null, {
                table,
                primaryKey,
                values: valuesCopy
            });
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

                const values = chunk.values.map(values => `(${Object.values(values).map(value => (value != null ? `'${value}'` : 'null')).join(', ')})`).join(',');
                const fields = Object.keys(chunk.values[0]).join(', ');
                const onConflict = (Array.isArray(chunk.primaryKey) ? chunk.primaryKey.join(', ') : chunk.primaryKey);
                const updateFields = Object.keys(chunk.values[1]).slice(0).map(value => `${value} = excluded.${value}`).join(', ');

                let newQuery = null;
                if (chunk.primaryKey) newQuery = `INSERT INTO ${chunk.table} (${fields}) VALUES ${values} ON CONFLICT (${onConflict}) DO UPDATE SET ${updateFields}`;
                else newQuery = `INSERT INTO ${chunk.table} (${fields}) VALUES ${values}`;

                try {
                    const queryPromise = client.query({
                        text: newQuery,
                        rowMode: 'array'
                    });
                    callback(null, queryPromise);
                    await queryPromise;
                } catch (ex) {
                    console.error(ex.stack);
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
                callback(ex);
            }
        }
    });
};

const createFileUnzipStream = (createFileProcessorStream, db, data) => {
    return new stream.Transform({
        objectMode: true,
        transform: async (entry, e, callback) => {
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
        }
    });
};

const createFileProcessorStream = (db, data, filePath) => {
    const createUnknownProgressStream = (name) => {
        const unknowProgress = progress({
            time: 100,
            objectMode: true
        });
        unknowProgress.on('progress', (progress) => {
            console.log(`${name} file ${filePath}: #${progress.transferred}`);
        });
        return unknowProgress;
    };
    const encodingTransformStream = createEncodingTransformStream();
    const csvParserStream = createCSVParserAndTransformerStream(data.metadata.formatMap);
    const batchTransformStream = createBatchTransformStream(data.metadata.table, data.metadata.upsertConflictField);
    const createInsertPromisesStream = createInsertPromiseStream(db.pool);
    const waitForPromisesStream = createWaitForPromisesStream();
    return [
        ...encodingTransformStream,
        csvParserStream,
        createUnknownProgressStream('Lines'),
        batchTransformStream,
        createUnknownProgressStream('Batch'),
        createInsertPromisesStream,
        createUnknownProgressStream('Query'),
        waitForPromisesStream
    ];
};

module.exports = async (db, dataList) => {
    const processedList = storeOrLoadProcessedList();

    console.time('Total processing time');

    const limit = promiseLimit(4);

    await Promise.all(dataList.map((data) => limit(async () => {
        console.time(`File ${data.url} processing time`);

        const fundListURL = data.url;

        if (await isDownloadNecessary(fundListURL, processedList)) {
            //if (true) {
            const progressStream = progress({
                time: 100,
                length: 'auto',
                drain: true
            });
            progressStream.on('progress', (progress) => {
                console.log(`Download file ${data.url}: ${progress.percentage.toFixed(2)}%, speed:${progress.speed.toFixed(2)} bytes/s`);
            });

            const client = await db.pool.connect();

            try {
                await client.query('BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');

                if (!data.metadata.upsertConflictField) await db.pool.query(`TRUNCATE ${data.metadata.table}`);

                const urlDownloadStream = createDownloadStream(fundListURL);

                if (path.extname(data.url).toLowerCase() == '.zip') {
                    await promisePipe(
                        //fs.createReadStream('./db/inf_diario_fi_2012.zip'),
                        urlDownloadStream,
                        progressStream,
                        unzipStream.Parse(),
                        createFileUnzipStream(createFileProcessorStream, db, data, data.url),
                        stream.Writable()
                    );
                } else {
                    await promisePipe(
                        //fs.createReadStream('./db/inf_diario_fi_201601.csv'),
                        urlDownloadStream,
                        progressStream,
                        ...createFileProcessorStream(db, data, data.url)
                    );
                }

                processedList[fundListURL] = urlDownloadStream.response.headers.etag;

                //storeOrLoadProcessedList(processedList);

                await client.query('COMMIT');
            } catch (ex) {
                await client.query('ROLLBACK');
                console.error(ex.stack);
                throw ex;
            } finally {
                client.release();
            }
        }

        console.timeEnd(`File ${data.url} processing time`);
    })));

    console.timeEnd('Total processing time');
};