const Db = require('../../util/db');

const got = require('got');
const stringSimilarity = require('string-similarity');
const allKeys = require('promise-results/allKeys');
const moment = require('moment');

const TransactionedWorker = require('../transactionedWorker');
const formatters = require('../../util/formatters');
const CONFIG = require('../../util/config');

class ModalMaisFundWorker extends TransactionedWorker {
    id = 'ModalMaisFundWorker'

    constructor() {
        super();
    }

    async getData(mainClient, progress) {
        progress.start();
        return getFundsData(mainClient);
    }

    async discovery(mainClient, progress, { funds, modalMaisFunds }) {
        return discoverFunds(funds, modalMaisFunds, progress);
    }

    async updateDatabase(mainClient, progress, rowsToUpsert) {
        progress.start();

        // Remove duplicate IDs
        rowsToUpsert = rowsToUpsert.reduce((r, i) => !r.some(j => i.mf_id === j.mf_id) ? [...r, i] : r, []);

        let newQuery = Db.createUpsertQuery({
            table: 'modalmais_funds',
            primaryKey: 'mf_id',
            values: rowsToUpsert
        });

        try {
            return await mainClient.query({
                text: newQuery,
                rowMode: 'array'
            });
        } catch (ex) {
            throw new Error(`Query "${CONFIG.QUERY_LOG_SIZE ? newQuery.substr(0, CONFIG.QUERY_LOG_SIZE) : newQuery}" failed`);
        }
    }
}

const getFundsData = async (client) => {
    const query = 'SELECT f_cnpj, f_short_name FROM funds';
    const URL = 'https://www.modalmais.com.br/wp-content/themes/Avada/js/modalmais/listaFundos.json?v=1&data';
    return allKeys({
        funds: client.query(query).catch(ex => { throw new Error(`Query "${query}" failed`, ex); }),
        modalMaisFunds: got.get(URL).then(result => JSON.parse(result.body)).catch(ex => { throw new Error(`HTTP get or parse of "${URL}" failed`, ex); })
    });
};

const discoverFunds = async (funds, modalMaisFunds, progress) => {
    progress.start(modalMaisFunds.length);

    const normalize = (name) => {
        return name.toLowerCase()
            .replace(/[àáâãäå]/gi, 'a')
            .replace(/æ/gi, 'ae')
            .replace(/ç/gi, 'c')
            .replace(/[èéêë]/gi, 'e')
            .replace(/[ìíîï]/gi, 'i')
            .replace(/ñ/gi, 'n')
            .replace(/[òóôõö]/gi, 'o')
            .replace(/œ/gi, 'oe')
            .replace(/[ùúûü]/gi, 'u')
            .replace(/[ýÿ]/gi, 'y')
            .replace(/(^| )(ficfim)( |$)/ig, '$1fic fim$3')
            .replace(/(^| )(ficfi)( |$)/ig, '$1fic fi$3')
            .replace(/(^| )(equity)( |$)/ig, '$1eq$3')
            .replace(/(^| )(equities)( |$)/ig, '$1eq$3')
            .replace(/(^| )(absoluto)( |$)/ig, '$1abs$3')
            .replace(/(^| )(fi cotas)( |$)/ig, '$1fic$3')
            .replace(/(^| )(fi acoes)( |$)/ig, '$1fia$3')
            .replace(/(^| )(cred priv)( |$)/ig, '$1cp$3')
            .replace(/(^| )(credito privado)( |$)/ig, '$1cp$3')
            .replace(/(^| )(cred corp)( |$)/ig, '$1cp$3')
            .replace(/(^| )(cred pri)( |$)/ig, '$1cp$3')
            .replace(/(^| )(deb in)( |$)/ig, '$1debenture incentivadas$3')
            .replace(/(^| )(deb inc)( |$)/ig, '$1debenture incentivadas$3')
            .replace(/(^| )(deb incent)( |$)/ig, '$1debenture incentivadas$3')
            .replace(/(^| )(deb incentivadas)( |$)/ig, '$1debenture incentivadas$3')
            .replace(/(^| )(infra incentivado)( |$)/ig, '$1debenture incentivadas$3')
            .replace(/(^| )(mult)( |$)/ig, '$1multistrategy$3')
            .replace(/(^| )(cap)( |$)/ig, '$1capital$3')
            .replace(/(^| )(small)( |$)/ig, '$1s$3')
            .replace(/(^| )(glob)( |$)/ig, '$1global$3')
            .replace(/(^| )(g)( |$)/ig, '$1global$3')
            .replace(/(^| )(de)( |$)/ig, ' ')
            .replace(/(^| )(acs)( |$)/ig, '$1access$3')
            .replace(/(^| )(cr)( |$)/ig, '$1credit$3')
            .replace(/(^| )(ref)( |$)/ig, '$1referenciado$3')
            .replace(/(^| )(fiq)( |$)/ig, '$1fic$3')
            .replace(/(^| )(fi multimercado)( |$)/ig, '$1fim$3')
            .replace(/(^| )(fi multimercad)( |$)/ig, '$1fim$3')
            .replace(/(^| )(fi multistrategy)( |$)/ig, '$1fim$3')
            .replace(/(^| )(lb)( |$)/ig, '$1long biased$3')
            .replace(/(^| )(ls)( |$)/ig, '$1long short$3')
            .replace(/(^| )(dl)( |$)/ig, '$1direct lending$3')
            .replace(/(^| )(fundo (?:de )?investimento)( |$)/ig, '$1fi$3')
            .replace(/(^| )(multimercado)( |$)/ig, '$1fim$3')
            .replace(/(^| )(acoes)( |$)/ig, '$1fia$3')
            .replace(/(^| )(renda fixa)( |$)/ig, '$1firf$3')
            .replace(/(^| )(investimento no exterior)( |$)/ig, '$1ie$3')
            .replace(/(^| )(fundo rf)( |$)/ig, '$1firf$3');
    };

    const avaliableFundsName = [];
    const avaliableFundsNameIndex = [];

    funds.rows.map((row, index) => {
        const normalizedName = normalize(row.f_short_name);
        avaliableFundsName.push(normalizedName);
        avaliableFundsNameIndex[normalizedName] = index;
    });

    let rowsToUpsert = [];

    for await (const item of modalMaisFunds) {
        const row = {
            mf_id: formatters.parseInt(item.InvestirLink),
            mf_cnpj: null,
            mf_date: (new Date()).toISOString(),
            mf_name: item.nome.replace(/'/g, ''),
            mf_risk_name: item.risco,
            mf_risk_level: formatters.parseInt(item.nivelRisco),
            mf_minimum_initial_investment: formatters.cleanBRMoney(item.aplicacaoMinima) / 100,
            mf_administration_fee: formatters.cleanBRMoney(item.taxaAdm) / 100,
            mf_start_date: item.startDate === '' ? null : moment.utc(item.dataInicio, 'DD/MM/YYYY').toISOString(),
            mf_target_audience: item.publicoAlvo,
            mf_profile: item.perfil,
            mf_net_equity: formatters.cleanBRMoney(item.PLmes) / 100,
            mf_net_equity_1y: formatters.cleanBRMoney(item.PL12meses) / 100,
            mf_benchmark: item.indexadorFundo,
            mf_rescue_quota: formatters.removeRelativeBRDate(item.CotaResgate),
            mf_rescue_financial_settlement: formatters.removeRelativeBRDate(item.PagamentoResgate),
            mf_minimum_moviment: typeof (item.MovimentacaoMinima) != 'undefined' && formatters.cleanBRMoney(item.MovimentacaoMinima) / 100,
            mf_investment_quota: typeof (item.CotacaoAplicacao) != 'undefined' && formatters.removeRelativeBRDate(item.CotacaoAplicacao),
            mf_minimal_amount_to_stay: typeof (item.SaldoMinimo) != 'undefined' && formatters.cleanBRMoney(item.SaldoMinimo) / 100,
            mf_max_administration_fee: typeof (item.TaxaMaxima) != 'undefined' && formatters.cleanBRMoney(item.TaxaMaxima) / 100,
            mf_performance_fee: typeof (item.TaxaPerform) != 'undefined' && formatters.cleanBRMoney(item.TaxaPerform) / 100,
            mf_tax_text: item.ImpostoRenda,
            mf_description: item.DescricaoFundo,
            mf_detail_link: item.linkDetalhe,
            mf_active: item.ativo == '1' ? true : false
        };

        const normalizedProduct = normalize(row.mf_name);
        const bestMatch = stringSimilarity.findBestMatch(normalizedProduct, avaliableFundsName).bestMatch;
        if (bestMatch.rating >= 0.80) {
            row.mf_cnpj = funds.rows[avaliableFundsNameIndex[bestMatch.target]].f_cnpj;
        } else {
            progress.log(`Not found: (${bestMatch.rating.toFixed(2)}): ${normalizedProduct} = ${bestMatch.target}`);
        }

        if (row.mf_id != null && row.mf_cnpj != null) rowsToUpsert.push(row);
        else progress.log(`Fund '${normalizedProduct}' id or cnpj is null, ignoring...`);

        progress.step();
        await progress.immediate();
    }

    return rowsToUpsert;
};

module.exports = ModalMaisFundWorker;