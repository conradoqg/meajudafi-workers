const stream = require('stream');

const createDummyWritableObjectStream = () => {
    return stream.Writable({ objectMode: true, write: (chunk, e, callback) => callback() });
};

module.exports = createDummyWritableObjectStream;