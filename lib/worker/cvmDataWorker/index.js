const Worker = require('../worker');
const Db = require('../../util/db');
const dataProcessor = require('../cvmStatisticWorker/dataProcessor');
const formatters = require('../../util/formatters');
const string = require('../../util/string');
const url = require('url');
const path = require('path');

const create_inf_diario_fi_urlList = () => {
    const template = (year, month) => `http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_${string.pad(year, 4)}${string.pad(month, 2)}.csv`;
    const templateHist = (year) => `http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_${string.pad(year, 4)}.zip`;

    const now = new Date();
    const months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'];
    const years = [now.getFullYear() - 1, now.getFullYear()];

    const urlList = years.map(year => months.reduce((result, month) => {
        if (year == now.getFullYear() && month > now.getMonth() + 1) return result;

        result.push(template(year, month));
        return result;
    }, [])).reduce((acc, val) => acc.concat(val), []);

    const yearsHist = Array((now.getFullYear() - 2 - 2005)).fill(null).reduce(result => {
        result.push(result[result.length - 1] + 1);
        return result;
    }, [2005]);

    const urlListHist = yearsHist.map(year => templateHist(year));

    return urlList.concat(urlListHist);
};

const create_fbcdata_sgs = (indicator) => {
    const template = (indicator) => `https://api.bcb.gov.br/dados/serie/bcdata.sgs.${indicator}/dados?formato=csv`;
    return template(indicator);
};

const create_inf_cadastral_fi_urlList = () => {
    const template = (year, month, day) => `http://dados.cvm.gov.br/dados/FI/CAD/DADOS/inf_cadastral_fi_${year}${string.pad(month, 2)}${string.pad(day, 2)}.csv`;

    const now = new Date();
    now.setDate(now.getDate() - 2);

    if (now.getDay() == 0 || now.getDay() == 6) {
        const t = new Date().getDate() + (6 - new Date().getDay() - 1) - 7;
        now.setDate(t);
    }

    return template(now.getFullYear(), now.getMonth() + 1, now.getDate());
};

class CVMDataWorker extends Worker {
    constructor() {
        super();
    }

    async work() {
        try {

            const db = new Db();

            await db.takeOnline();

            try {

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
                    }
                };

                const inf_diario_fi_metadata = {
                    table: 'inf_diario_fi',
                    upsertConflictField: ['CNPJ_FUNDO', 'DT_COMPTC'],
                    formatMap: {
                        'CNPJ_FUNDO': formatters.cleanCNPJ,
                        'DT_COMPTC': formatters.parseDate,
                        'VL_TOTAL': formatters.parseFloat,
                        'VL_QUOTA': formatters.parseQuota,
                        'VL_PATRIM_LIQ': formatters.parseFloat,
                        'CAPTC_DIA': formatters.parseFloat,
                        'RESG_DIA': formatters.parseFloat,
                        'NR_COTST': formatters.parseInt,
                    }
                };

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

                const inf_diario_fi_urlList = create_inf_diario_fi_urlList();
                const inf_cadastral_fi_urlList = create_inf_cadastral_fi_urlList();
                const fbcdata_sgs_12i = create_fbcdata_sgs(12); // CDI
                const fbcdata_sgs_11i = create_fbcdata_sgs(11); // SELIC
                const fbcdata_sgs_7i = create_fbcdata_sgs(7); // Bovespa
                const fbcdata_sgs_433i = create_fbcdata_sgs(433); // IPCA
                const fbcdata_sgs_189i = create_fbcdata_sgs(189); // IGP-M
                const fbcdata_sgs_190i = create_fbcdata_sgs(190); // IGP-DI
                const fbcdata_sgs_1i = create_fbcdata_sgs(1); // Dolar
                const fbcdata_sgs_21619i = create_fbcdata_sgs(21619); // Euro

                let dataList = [];

                dataList.push(...inf_diario_fi_urlList.map(urlBase => { return { name: path.basename(url.parse(urlBase).pathname), url: urlBase, metadata: inf_diario_fi_metadata }; }));
                dataList.push({ name: 'inf_cadastral_fi', url: inf_cadastral_fi_urlList, metadata: inf_cadastral_fi_metadata });
                dataList.push({ name: 'fbcdata_sgs_12i', url: fbcdata_sgs_12i, metadata: fbcdata_sgs_12i_metadata });
                dataList.push({ name: 'fbcdata_sgs_11i', url: fbcdata_sgs_11i, metadata: fbcdata_sgs_11i_metadata });
                dataList.push({ name: 'fbcdata_sgs_7i', url: fbcdata_sgs_7i, metadata: fbcdata_sgs_7i_metadata });
                dataList.push({ name: 'fbcdata_sgs_433i', url: fbcdata_sgs_433i, metadata: fbcdata_sgs_433i_metadata });
                dataList.push({ name: 'fbcdata_sgs_189i', url: fbcdata_sgs_189i, metadata: fbcdata_sgs_189i_metadata });
                dataList.push({ name: 'fbcdata_sgs_190i', url: fbcdata_sgs_190i, metadata: fbcdata_sgs_190i_metadata });
                dataList.push({ name: 'fbcdata_sgs_1i', url: fbcdata_sgs_1i, metadata: fbcdata_sgs_1i_metadata });
                dataList.push({ name: 'fbcdata_sgs_21619i', url: fbcdata_sgs_21619i, metadata: fbcdata_sgs_21619i_metadata });

                //dataList = dataList.slice(0, 5);

                //dataList.push({ url: 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_2015.zip', metadata: inf_diario_fi_metadata })

                await dataProcessor(db, dataList);
            } catch (ex) {
                console.error(ex.stack);
                throw ex;
            } finally {
                await db.takeOffline();
            }
        } catch (ex) {
            console.error(ex.stack);
        }
    }
}

module.exports = CVMDataWorker;