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

module.exports = async (pool, createDataParser, dataList, progress) => {
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
                    const downloadProgress = new DefaultProgress(`${progress.progressTracker.id}.download.${data.name}`, prettyBytes);

                    let retriesLeft = CONFIG.downloadRetries;
                    let success = false;
                    while (retriesLeft-- > 0) {
                        const urlDownloadStream = createDownloadStream(fundListURL);
                        const downloadProgressStream = progressStream({
                            time: 500,
                            length: 'auto',
                            drain: true
                        });
                        downloadProgressStream.on('progress', (current) => {
                            if (downloadProgress.progressTracker.state.status == 'new') downloadProgress.start(current.length);
                            downloadProgress.set(current.transferred);
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
                            downloadProgress.log(`Warning: Download try failed: ${ex.stack}`);
                            downloadProgress.error();
                        }
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

                        if (path.extname(data.url).toLowerCase() == '.zip') {
                            await promisePipe(
                                fs.createReadStream(localFilename),
                                readProgressStream,
                                unzipStream.Parse(),
                                createFileUnzipStream(createFileProcessorStream, createDataParser, data, client, progress),
                                stream.Writable()
                            );
                        } else {
                            await promisePipe(
                                fs.createReadStream(localFilename),
                                readProgressStream,
                                ...createFileProcessorStream(createDataParser, data, client, progress)
                            );
                        }

                        transformProgress.end();

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
            if (!isClientErrored) await client.query('ROLLBACK');
            throw ex;
        } finally {
            await client.release();
        }

        progress.step();
    }
};