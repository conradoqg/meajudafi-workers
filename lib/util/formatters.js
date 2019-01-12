const moment = require('moment');

const CNPJ_PONCTUATION = /[./-]/g;
const PONCTUATION = /\./g;
const numericalCleanerRegex = /[.,%R$]/g;

const parseIntEx = (value) => {
    const parsedValue = parseInt(value);
    return !isNaN(parsedValue) ? parsedValue : null;
};

const parseFloatEx = (value) => {
    const parsedValue = parseFloat(value);
    return !isNaN(parsedValue) ? parsedValue : null;
};

const formatters = {
    cleanCNPJ: (value) => (value ? value.replace(CNPJ_PONCTUATION, '') : value),
    parseInt: parseIntEx,
    parseFloat: parseFloatEx,
    TAXA_PERFM: (value) => {
        const firstSpace = value.indexOf(' ');
        if (firstSpace > 0) return parseIntEx(value.substr(0, firstSpace - 1));
        else return parseIntEx(value);
    },
    parseDate: (value) => value ? value : null,
    parseBRDate: (value) => {
        return value ? moment(value, 'DD/MM/YYYY').format('YYYY-MM-DD') : null;
    },
    parseBRFloat: (value) => parseFloatEx(value.replace(/,/g, '.')),    
    cleanBRMoney: (value) => parseFloatEx(value.replace(numericalCleanerRegex, '')),
    removeRelativeBRDate: (value) => parseIntEx(value.replace('D+', ''))
};

module.exports = formatters;