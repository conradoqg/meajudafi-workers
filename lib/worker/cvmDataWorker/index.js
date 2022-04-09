const url = require('url');
const path = require('path');

const PooledWorker = require('../pooledWorker');
const dataProcessor = require('../../processor/dataProcessor');
const formatters = require('../../util/formatters');
const string = require('../../util/string');
const createCSVParserAndTransformerStream = require('../../stream/createCSVParserAndTransformerStream');

const create_inf_diario_fi_urlList = () => {
    const template = (year, month) => `http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_${string.pad(year, 4)}${string.pad(month, 2)}.csv`;
    const templateHist = (year) => `http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_${string.pad(year, 4)}.zip`;

    const now = new Date();
    const months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'];
    const years = [now.getFullYear() - 1, now.getFullYear()];

    let urlList = years.map(year => months.reduce((result, month) => {
        if (year == now.getFullYear() && month > now.getMonth() + 1) return result;

        result.push(template(year, month));
        return result;
    }, [])).reduce((acc, val) => acc.concat(val), []);

    const yearsHist = Array((now.getFullYear() - 2 - 2005)).fill(null).reduce(result => {
        result.push(result[result.length - 1] + 1);
        return result;
    }, [2005]);

    const urlListHist = yearsHist.map(year => templateHist(year));

    urlList = urlListHist.concat(urlList);

    return urlList;
};

class CVMDataWorker extends PooledWorker {
    id = 'CVMDataWorker'

    constructor() {
        super();
    }

    async createList(pool, progress, options) {
        progress.start();
        let sample = false;
        if (options.options && options.options.includes('sampleData')) sample = true;

        const inf_cadastral_fi_metadata = {
            table: 'inf_cadastral_fi',
            formatMap: {
                'CNPJ_FUNDO': formatters.cleanCNPJ,
                'DT_REG': formatters.parseDate,
                'DT_CONST': formatters.parseDate,
                'DT_CANCEL': formatters.parseDate,
                'DT_INI_SIT': formatters.parseDate,
                'DT_INI_ATIV': formatters.parseDate,
                'DT_INI_EXERC': formatters.parseDate,
                'DT_FIM_EXERC': formatters.parseDate,
                'DT_INI_CLASSE': formatters.parseDate,
                'DT_PATRIM_LIQ': formatters.parseDate,
                'CNPJ_ADMIN': formatters.cleanCNPJ,
                'CPF_CNPJ_GESTOR': formatters.cleanCNPJ,
                'CNPJ_AUDITOR': formatters.cleanCNPJ,
                'VL_PATRIM_LIQ': formatters.parseFloat,
                'TAXA_PERFM': formatters.TAXA_PERFM
            },
            quote: false
        };

        const inf_diario_fi_metadata = {
            table: 'inf_diario_fi',
            upsertConflictField: ['CNPJ_FUNDO', 'DT_COMPTC'],
            formatMap: {
                'CNPJ_FUNDO': formatters.cleanCNPJ,
                'DT_COMPTC': formatters.parseDate,
                'VL_TOTAL': formatters.parseFloat,
                'VL_QUOTA': formatters.parseFloat,
                'VL_PATRIM_LIQ': formatters.parseFloat,
                'CAPTC_DIA': formatters.parseFloat,
                'RESG_DIA': formatters.parseFloat,
                'NR_COTST': formatters.parseInt,
            }
        };

        const inf_diario_fi_urlList = create_inf_diario_fi_urlList();

        let dataList = [];

        let inf_diario = inf_diario_fi_urlList.map(urlBase => { return { name: path.basename(url.parse(urlBase).pathname), url: urlBase, metadata: inf_diario_fi_metadata }; });

        if (sample) inf_diario = inf_diario.slice(inf_diario.length - 5);

        dataList.push(...inf_diario);
        dataList.push({ name: 'inf_cadastral_fi', url: 'http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv', metadata: inf_cadastral_fi_metadata });

        const createDataParser = (data) => [createCSVParserAndTransformerStream(data.metadata.formatMap, data.metadata.delimiter, data.metadata.defaultData, data.metadata.validLine, data.metadata.quote)];

        return { createDataParser, dataList };
    }

    async processData(pool, progress, { createDataParser, dataList }) {
        return dataProcessor(pool, createDataParser, dataList, progress);
    }
}

module.exports = CVMDataWorker;