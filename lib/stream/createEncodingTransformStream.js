const iconv = require('iconv-lite');

const createEncodingTransformStream = () => {
    return [iconv.decodeStream('WINDOWS-1252'), iconv.encodeStream('UTF-8')];
};

module.exports = createEncodingTransformStream;