const got = require('got');

const createDownloadStream = (url) => {
    return got.stream(url);
};

module.exports = createDownloadStream;