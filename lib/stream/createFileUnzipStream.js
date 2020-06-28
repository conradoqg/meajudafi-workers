const stream = require('stream');
const promisePipe = require('promisepipe');
const path = require('path');

const createFileUnzipStream = (createFileProcessorStream, createDataParser, data, client, progress) => {
    return new stream.Transform({
        objectMode: true,
        transform: async (entry, e, callback) => {
            try {
                const filePath = entry.path;
                const type = entry.type;
                if (type == 'File' && (path.extname(filePath).toLowerCase() == '.csv' || path.extname(filePath).toLowerCase() == '.txt' || path.extname(filePath).slice(1,2).toLowerCase() == 'a')) {
                    try {
                        await promisePipe(
                            entry,
                            ...createFileProcessorStream(createDataParser, data, client, progress)
                        );
                        callback();
                    } catch (ex) {
                        entry.autodrain();
                        console.error('Error: createFileUnzipStream: pipe');
                        console.error(ex.stack);
                        callback(ex);
                    }
                } else {
                    entry.autodrain();
                    callback();
                }
            } catch (ex) {
                console.error('Error: createFileUnzipStream: transform');
                console.error(ex.stack);
            }
        }
    });
};

module.exports = createFileUnzipStream;