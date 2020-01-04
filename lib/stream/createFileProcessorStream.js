const createWaitForPromisesStream = require('./createWaitForPromisesStream');
const createBatchTransformStream = require('./createBatchTransformStream');
const createEncodingTransformStream = require('./createEncodingTransformStream');
const createInsertPromiseStream = require('./createInsertPromiseStream');
const createCSVParserAndTransformerStream = require('./createCSVParserAndTransformerStream');

const createFileProcessorStream = (db, data) => {
    const encodingTransformStream = createEncodingTransformStream();
    const csvParserStream = createCSVParserAndTransformerStream(data.metadata.formatMap, data.metadata.delimiter);
    const batchTransformStream = createBatchTransformStream(data.metadata.table, data.metadata.upsertConflictField);
    const createInsertPromisesStream = createInsertPromiseStream(db);
    const waitForPromisesStream = createWaitForPromisesStream();
    return [
        ...encodingTransformStream,
        csvParserStream,
        batchTransformStream,
        createInsertPromisesStream,
        waitForPromisesStream
    ];
};

module.exports = createFileProcessorStream;