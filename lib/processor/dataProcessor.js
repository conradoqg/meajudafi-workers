const Db = require('../util/db');

const fs = require('fs-extra');
const promisePipe = require('promisepipe');
const stream = require('stream');
const progressStream = require('progress-stream');
const unzipStream = require('unzip-stream');
const path = require('path');
const got = require('got');
const prettyBytes = require('pretty-bytes');
const crypto = require('crypto');

const CONFIG = require('../util/config');
const createDownloadStream = require('../stream/createDownloadStream');
const createFileUnzipStream = require('../stream/createFileUnzipStream');
const createFileProcessorStream = require('../stream/createFileProcessorStream');
const DefaultProgress = require('../progress/defaultProgress');

const dataProcessor = async (pool, createDataParser, dataList, progress) => {
    progress.start(dataList.length);

    let isClientErrored = false;

    for (const data of dataList) {
        const fundListURL = data.url;

        const client = await pool.connect();
        client.on('error', err => {
            progress.log(`Database client errored: ${err.stack}`);
            isClientErrored = true;
        });


        try {
            let shouldDownload = true;            
            let lastModified = null;

            try {
                const result = await got.head(fundListURL);

                if (result.statusCode && result.statusCode == 200) {                    
                    lastModified = result.headers['last-modified'] ? result.headers['last-modified'] : null;
                }

                const foundProcessedURLResult = await client.query(`SELECT last_modified from processed_url where url='${fundListURL}'`);
                if (foundProcessedURLResult.rowCount > 0) {
                    // For legacy compatibility, only considers last_modified if it has a value (because it was processed before with this new method)
                    if (lastModified && foundProcessedURLResult.rows[0].last_modified != null) {
                        if (foundProcessedURLResult.rows[0].last_modified == lastModified) shouldDownload = false;
                    }
                }
            } catch (ex) {
                throw new Error(`Should download ${fundListURL} failed`, ex);
            }            

            if (shouldDownload) {
                await client.query('BEGIN TRANSACTION');

                if (!data.metadata.upsertConflictField) await client.query(`DELETE FROM ${data.metadata.table}`);

                const tmpPath = './tmp/';

                fs.ensureDirSync(tmpPath);

                const localFilename = tmpPath + crypto.createHash('sha1').update(fundListURL).digest('hex');

                try {
                    const downloadProgress = new DefaultProgress(`${progress.progressTracker.id}.download.${data.name}`, prettyBytes);

                    let retriesLeft = CONFIG.downloadRetries;
                    let success = false;
                    let lastError = null;

                    while (retriesLeft-- > 0) {
                        const urlDownloadStream = createDownloadStream(fundListURL);
                        const downloadProgressStream = progressStream({
                            time: 500,
                            length: 'auto',
                            drain: true
                        });
                        downloadProgressStream.on('progress', (current) => {
                            if (downloadProgress.progressTracker.state.status == 'new' && current.length == 'auto') downloadProgress.start();
                            else {
                                if (downloadProgress.progressTracker.state.status == 'new') downloadProgress.start(current.length);
                                downloadProgress.set(current.transferred);
                            }
                        });
                        try {
                            await promisePipe(
                                urlDownloadStream,
                                downloadProgressStream,
                                fs.createWriteStream(localFilename)
                            );
                            success = true;
                            break;
                        } catch (ex) {
                            downloadProgress.log(`HTTP get of "${fundListURL}" failed, ${retriesLeft} retries left, waiting 1000ms for next try...`);
                            await progress.delay(1000);
                            lastError = ex.originalError;
                        }
                    }

                    if (!success) {
                        downloadProgress.error();
                        throw new Error(`HTTP get "${data.url}" failed`, lastError);
                    }

                    downloadProgress.end();

                    if (success) {
                        const stat = fs.statSync(localFilename);

                        const transformProgress = new DefaultProgress(`${progress.progressTracker.id}.transform.${data.name}`, prettyBytes);

                        transformProgress.start(stat.size);

                        const readProgressStream = progressStream({
                            time: 500,
                            length: stat.size
                        });
                        readProgressStream.on('progress', (current) => transformProgress.set(current.transferred));

                        try {
                            if (path.extname(data.url).toLowerCase() == '.zip') {
                                await promisePipe(
                                    fs.createReadStream(localFilename),
                                    readProgressStream,
                                    unzipStream.Parse(),
                                    createFileUnzipStream(createFileProcessorStream, createDataParser, data, client),
                                    stream.Writable()
                                );
                            } else {
                                await promisePipe(
                                    fs.createReadStream(localFilename),
                                    readProgressStream,
                                    ...createFileProcessorStream(createDataParser, data, client)
                                );
                            }
                        } catch (ex) {
                            throw new Error(`Transformation of ${data.url} failed`, ex.originalError);
                        }

                        transformProgress.end();

                        const processedURLUpsertQuery = Db.createUpsertQuery({
                            table: 'processed_url',
                            primaryKey: ['url'],
                            values: [{
                                'url': fundListURL,                                
                                'last_modified': lastModified
                            }]
                        });

                        await client.query(processedURLUpsertQuery);

                        await client.query('COMMIT');
                    }
                } finally {
                    await fs.unlink(localFilename);
                }
            }
        } catch (ex) {
            if (!isClientErrored) await client.query('ROLLBACK');
            throw ex;
        } finally {
            await client.release();
        }

        progress.step();
    }
};

module.exports = dataProcessor;