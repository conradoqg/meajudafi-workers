const createBatchTransformStream = require('./createBatchTransformStream');
const createEncodingTransformStream = require('./createEncodingTransformStream');
const createInsertPromiseStream = require('./createInsertPromiseStream');

const createFileProcessorStream = (createDataParserStream, data, client) => {
    const encodingTransformStream = createEncodingTransformStream();
    const dataParserStream = createDataParserStream(data);
    const batchTransformStream = createBatchTransformStream(data.metadata.table, data.metadata.upsertConflictField);
    const createInsertPromisesStream = createInsertPromiseStream(client);    
    return [
        ...encodingTransformStream,
        ...dataParserStream,
        batchTransformStream,
        createInsertPromisesStream
    ];
};

module.exports = createFileProcessorStream;