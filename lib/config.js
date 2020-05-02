module.exports = {
    batchSize: 300,
    highWaterMark: 1000,
    downloadRetries: 3,
    scraperTries: 5,
    CONNECTION_STRING: 'postgresql://postgres:temporary@meajudafi.com.br:5432/cvmData',
    READONLY_USERNAME: 'readonly',
    READONLY_PASSWORD: 'pJvykeLXdhCDFs99',
    WTD_TOKEN: null,
    EOD_TOKEN: null,
    POOL_SIZE: 20,
    MAX_PARALLEL_THREADS: 16,
    PARALLEL_PROCESS: true
};