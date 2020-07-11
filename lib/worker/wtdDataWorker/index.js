const PooledWorker = require('../pooledWorker');
const dataProcessor = require('../../processor/dataProcessor');
const formatters = require('../../util/formatters');
const CONFIG = require('../../util/config');
const createCSVParserAndTransformerStream = require('../../stream/createCSVParserAndTransformerStream');

class WTDDataWorker extends PooledWorker {
    id = 'WTDDataWorker'

    constructor() {
        super();
    }

    async createList(pool, progress) {
        progress.start();


        const wtd_ibov_metadata = {
            table: 'wtd_ibov',
            upsertConflictField: ['Date'],
            delimiter: ',',
            formatMap: {
                'Date': formatters.parseDate,
                'Open': formatters.parseFloat,
                'Close': formatters.parseFloat,
                'High': formatters.parseFloat,
                'Low': formatters.parseFloat,
                'Volume': formatters.parseFloat
            }
        };

        const wtd_ibov = CONFIG.WTD_TOKEN ? `https://api.worldtradingdata.com/api/v1/history?symbol=^IBOV&sort=newest&output=csv&api_token=${CONFIG.WTD_TOKEN}` : null;

        let dataList = [];

        if (wtd_ibov) dataList.push({ name: 'wtd_ibov', url: wtd_ibov, metadata: wtd_ibov_metadata });

        const createDataParser = (data) => [createCSVParserAndTransformerStream(data.metadata.formatMap, data.metadata.delimiter, data.metadata.defaultData, data.metadata.validLine)];

        return { createDataParser, dataList };
    }

    async processData(pool, progress, { createDataParser, dataList }) {
        return dataProcessor(pool, createDataParser, dataList, progress);
    }
}

module.exports = WTDDataWorker;