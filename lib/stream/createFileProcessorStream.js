const createWaitForPromisesStream = require('./createWaitForPromisesStream');
const createBatchTransformStream = require('./createBatchTransformStream');
const createEncodingTransformStream = require('./createEncodingTransformStream');
const createInsertPromiseStream = require('./createInsertPromiseStream');

const createFileProcessorStream = (db, createDataParserStream, data) => {
    const encodingTransformStream = createEncodingTransformStream();
    const dataParserStream = createDataParserStream(data);
    const batchTransformStream = createBatchTransformStream(data.metadata.table, data.metadata.upsertConflictField);
    const createInsertPromisesStream = createInsertPromiseStream(db);
    const waitForPromisesStream = createWaitForPromisesStream();
    return [
        ...encodingTransformStream,
        ...dataParserStream,
        batchTransformStream,
        createInsertPromisesStream,
        waitForPromisesStream
    ];
};

module.exports = createFileProcessorStream;