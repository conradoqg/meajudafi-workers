const stream = require('stream');
const promisePipe = require('promisepipe');
const path = require('path');

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