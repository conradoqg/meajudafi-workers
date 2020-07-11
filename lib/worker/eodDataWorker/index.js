const PooledWorker = require('../pooledWorker');
const dataProcessor = require('../../processor/dataProcessor');
const formatters = require('../../util/formatters');
const CONFIG = require('../../util/config');
const createCSVParserAndTransformerStream = require('../../stream/createCSVParserAndTransformerStream');

class EODDataWorker extends PooledWorker {
    id = 'EODDataWorker'

    constructor() {
        super();
    }

    async createList(pool, progress) {
        progress.start();

        const eod_historial_data_metadata = {
            table: 'eod_historial_data',
            upsertConflictField: ['Symbol', 'Date'],
            delimiter: ',',
            defaultData: {
                'Symbol': 'BVSP.INDX'
            },
            validLine: (line) => {
                return line && line.Close != null;
            },
            formatMap: {
                'Date': formatters.parseDate,
                'Open': formatters.parseFloat,
                'Close': formatters.parseFloat,
                'Adjusted_close': formatters.parseFloat,
                'High': formatters.parseFloat,
                'Low': formatters.parseFloat,
                'Volume': formatters.parseFloat
            }
        };

        const eod_historial_data = CONFIG.EOD_TOKEN ? `https://eodhistoricaldata.com/api/eod/BVSP.INDX?from=2001-01-01&api_token=${CONFIG.EOD_TOKEN}` : null;

        let dataList = [];

        if (eod_historial_data) dataList.push({ name: 'eod_historial_data', url: eod_historial_data, metadata: eod_historial_data_metadata });

        const createDataParser = (data) => [createCSVParserAndTransformerStream(data.metadata.formatMap, data.metadata.delimiter, data.metadata.defaultData, data.metadata.validLine)];

        return { createDataParser, dataList };
    }

    async processData(pool, progress, { createDataParser, dataList }) {
        return dataProcessor(pool, createDataParser, dataList, progress);
    }
}

module.exports = EODDataWorker;