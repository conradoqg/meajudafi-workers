const PooledWorker = require('../pooledWorker');
const dataProcessor = require('../../processor/dataProcessor');
const formatters = require('../../util/formatters');
const createCSVParserAndTransformerStream = require('../../stream/createCSVParserAndTransformerStream');

const create_fbcdata_sgs = (indicator) => {
    const template = (indicator) => `https://api.bcb.gov.br/dados/serie/bcdata.sgs.${indicator}/dados?formato=csv`;
    return template(indicator);
};

class BCBDataWorker extends PooledWorker {
    id = 'BCBDataWorker'

    constructor() {
        super();
    }

    async createList(pool, progress) {
        progress.start();

        const fbcdata_sgs_12i_metadata = {
            table: 'fbcdata_sgs_12i',
            upsertConflictField: ['data'],
            formatMap: {
                'data': formatters.parseBRDate,
                'valor': formatters.parseBRFloat
            }
        };

        const fbcdata_sgs_11i_metadata = {
            table: 'fbcdata_sgs_11i',
            upsertConflictField: ['data'],
            formatMap: {
                'data': formatters.parseBRDate,
                'valor': formatters.parseBRFloat
            }
        };

        const fbcdata_sgs_7i_metadata = {
            table: 'fbcdata_sgs_7i',
            upsertConflictField: ['data'],
            formatMap: {
                'data': formatters.parseBRDate,
                'valor': formatters.parseBRFloat
            }
        };

        const fbcdata_sgs_433i_metadata = {
            table: 'fbcdata_sgs_433i',
            upsertConflictField: ['data'],
            formatMap: {
                'data': formatters.parseBRDate,
                'valor': formatters.parseBRFloat
            }
        };

        const fbcdata_sgs_189i_metadata = {
            table: 'fbcdata_sgs_189i',
            upsertConflictField: ['data'],
            formatMap: {
                'data': formatters.parseBRDate,
                'valor': formatters.parseBRFloat
            }
        };

        const fbcdata_sgs_190i_metadata = {
            table: 'fbcdata_sgs_190i',
            upsertConflictField: ['data'],
            formatMap: {
                'data': formatters.parseBRDate,
                'valor': formatters.parseBRFloat
            }
        };

        const fbcdata_sgs_1i_metadata = {
            table: 'fbcdata_sgs_1i',
            upsertConflictField: ['data'],
            formatMap: {
                'data': formatters.parseBRDate,
                'valor': formatters.parseBRFloat
            }
        };

        const fbcdata_sgs_21619i_metadata = {
            table: 'fbcdata_sgs_21619i',
            upsertConflictField: ['data'],
            formatMap: {
                'data': formatters.parseBRDate,
                'valor': formatters.parseBRFloat
            }
        };

        const fbcdata_sgs_12i = create_fbcdata_sgs(12); // CDI
        const fbcdata_sgs_11i = create_fbcdata_sgs(11); // SELIC
        const fbcdata_sgs_7i = create_fbcdata_sgs(7); // Bovespa
        const fbcdata_sgs_433i = create_fbcdata_sgs(433); // IPCA
        const fbcdata_sgs_189i = create_fbcdata_sgs(189); // IGP-M
        const fbcdata_sgs_190i = create_fbcdata_sgs(190); // IGP-DI
        const fbcdata_sgs_1i = create_fbcdata_sgs(1); // Dolar
        const fbcdata_sgs_21619i = create_fbcdata_sgs(21619); // Euro       

        let dataList = [];

        dataList.push({ name: 'fbcdata_sgs_12i', url: fbcdata_sgs_12i, metadata: fbcdata_sgs_12i_metadata });
        dataList.push({ name: 'fbcdata_sgs_11i', url: fbcdata_sgs_11i, metadata: fbcdata_sgs_11i_metadata });
        dataList.push({ name: 'fbcdata_sgs_7i', url: fbcdata_sgs_7i, metadata: fbcdata_sgs_7i_metadata });
        dataList.push({ name: 'fbcdata_sgs_433i', url: fbcdata_sgs_433i, metadata: fbcdata_sgs_433i_metadata });
        dataList.push({ name: 'fbcdata_sgs_189i', url: fbcdata_sgs_189i, metadata: fbcdata_sgs_189i_metadata });
        dataList.push({ name: 'fbcdata_sgs_190i', url: fbcdata_sgs_190i, metadata: fbcdata_sgs_190i_metadata });
        dataList.push({ name: 'fbcdata_sgs_1i', url: fbcdata_sgs_1i, metadata: fbcdata_sgs_1i_metadata });
        dataList.push({ name: 'fbcdata_sgs_21619i', url: fbcdata_sgs_21619i, metadata: fbcdata_sgs_21619i_metadata });

        const createDataParser = (data) => [createCSVParserAndTransformerStream(data.metadata.formatMap, data.metadata.delimiter, data.metadata.defaultData, data.metadata.validLine)];

        return { createDataParser, dataList };
    }

    async processData(pool, progress, { createDataParser, dataList }) {
        return dataProcessor(pool, createDataParser, dataList, progress);
    }
}

module.exports = BCBDataWorker;