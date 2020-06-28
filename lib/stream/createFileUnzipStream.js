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
                
                if (type == 'File' && (path.extname(filePath).toLowerCase() == '.csv' || path.extname(filePath).toLowerCase() == '.txt' || path.extname(filePath).slice(1, 2).toLowerCase() == 'a')) {
                    try {
                        await promisePipe(
                            entry,
                            ...createFileProcessorStream(createDataParser, data, client, progress)
                        );
                        callback();
                    } catch (ex) {
                        entry.autodrain();
                        callback(new Error(`Process of ${filePath} failed`, ex.originalError));
                    }
                } else {
                    entry.autodrain();
                    callback();
                }
            } catch (ex) {
                callback(ex);
            }
        }
    });
};

module.exports = createFileUnzipStream;