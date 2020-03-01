const Worker = require('../worker');
const Db = require('../../util/db');
const dataProcessor = require('../../processor/dataProcessor');
const formatters = require('../../util/formatters');
const createB3ParserAndTransformerStream = require('../../stream/createB3ParserAndTransformerStream');

class B3DataWorker extends Worker {
    constructor() {
        super();
    }

    async work(options) {
        try {

            let sample = false;
            if (options.options && options.options.includes('sampleData')) sample = true;

            const db = new Db();

            await db.takeOnline();

            try {
                const cota_hist_a_metadata = {
                    table: 'cota_hist_a',
                    upsertConflictField: ['CODNEG', 'DATA'],
                    formatMap: {
                        'DATA': formatters.parseDate,
                        'PREABE': formatters.parseFloat,
                        'PREMAX': formatters.parseFloat,
                        'PREMIN': formatters.parseFloat,
                        'PREMED': formatters.parseFloat,
                        'PREULT': formatters.parseFloat,
                        'PREOFC': formatters.parseFloat,
                        'PREOFV': formatters.parseFloat,
                        'TOTNEG': formatters.parseInt,
                        'QUATOT': formatters.parseInt,
                        'VOLTOT': formatters.parseFloat,
                        'PREEXE': formatters.parseFloat,
                        'DATVEN': formatters.parseDate
                    }
                };

                const availableYears = [...Array((new Date()).getFullYear() - (1986 - 1))].map((v, index) => 1986 + index);

                let dataList = availableYears.map(item => { return { name: `cota_hist_a${item}`, url: `http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A${item}.zip`, metadata: cota_hist_a_metadata }; });

                if (sample) dataList = dataList.slice(dataList.length - 1);

                const createDataParser = (data) => [...createB3ParserAndTransformerStream(data.metadata.formatMap)];

                await dataProcessor(db, createDataParser, dataList);
            } finally {
                await db.takeOffline();
            }
        } catch (ex) {
            console.error('Error: CVMDataWorker: work');
            console.error(ex.stack);
        }
    }
}

module.exports = B3DataWorker;