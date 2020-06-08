const baseConfig = require('../../config.json');

const handler = {
    get: (obj, prop) => {
        if (prop in process.env) return process.env[prop];
        else if (prop in obj) return obj[prop];             
    }
};

module.exports = new Proxy(baseConfig, handler);