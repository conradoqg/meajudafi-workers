const PooledWorker = require('../pooledWorker');
const dataProcessor = require('../../processor/dataProcessor');
const formatters = require('../../util/formatters');
const createCSVParserAndTransformerStream = require('../../stream/createCSVParserAndTransformerStream');
const dayjs = require('dayjs');
const querystring = require('querystring');

class YahooDataWorker extends PooledWorker {
    id = 'YahooDataWorker'

    constructor() {
        super();
    }

    async createList(pool, progress) {
        progress.start();

        const buildMetadata = (symbol) => ({
            table: 'yahoo_data',
            upsertConflictField: ['Symbol', 'Date'],
            delimiter: ',',
            defaultData: {
                'Symbol': symbol
            },
            validLine: (line) => {
                return line && line.Close != null;
            },
            formatMap: {
                'Date': formatters.parseDate,
                'Open': formatters.parseFloat,
                'High': formatters.parseFloat,
                'Low': formatters.parseFloat,
                'Close': formatters.parseFloat,
                'Adj Close': formatters.parseFloat,
                'Volume': formatters.parseFloat
            }
        });

        const startDate = dayjs('2000-01-01');
        const endDate = dayjs();

        const buildURL = (symbol) => `https://query1.finance.yahoo.com/v7/finance/download/${querystring.escape(symbol)}?period1=${startDate.unix()}&period2=${endDate.unix()}&interval=1d&events=history&includeAdjustedClose=true`;

        const symbols = ['^BVSP'];

        const dataList = symbols.map(symbol => ({
            name: `yahoo_data_${symbol}`,
            url: buildURL(symbol),
            metadata: buildMetadata(symbol)
        }));        

        const createDataParser = (data) => [createCSVParserAndTransformerStream(data.metadata.formatMap, data.metadata.delimiter, data.metadata.defaultData, data.metadata.validLine)];

        return { createDataParser, dataList };
    }

    async processData(pool, progress, { createDataParser, dataList }) {
        return dataProcessor(pool, createDataParser, dataList, progress);
    }
}

module.exports = YahooDataWorker;