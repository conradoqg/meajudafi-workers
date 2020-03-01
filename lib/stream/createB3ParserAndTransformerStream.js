const LineStream = require('byline').LineStream;

const CONFIG = require('../config');
const stream = require('stream');

const QUOTE = /'/g;

const createB3ParserAndTransformerStream = (formatMap) => {

    const b3LineParser = (chunk) => {
        const line = chunk.toString();

        if (line.slice(0, 2) !== '01') return;

        let data = {
            DATA: line.slice(2, 10),
            CODBDI: line.slice(10, 12),
            CODNEG: line.slice(12, 24).toUpperCase(),
            TPMERC: line.slice(24, 27),
            NOMRES: line.slice(27, 39),
            ESPECI: line.slice(39, 49),
            PRAZOT: line.slice(49, 52),
            MODREF: line.slice(52, 56),
            PREABE: line.slice(56, 69),
            PREMAX: line.slice(69, 82),
            PREMIN: line.slice(82, 95),
            PREMED: line.slice(96, 108),
            PREULT: line.slice(109, 121),
            PREOFC: line.slice(121, 134),
            PREOFV: line.slice(134, 147),
            TOTNEG: line.slice(147, 152),
            QUATOT: line.slice(152, 170),
            VOLTOT: line.slice(170, 188),
            PREEXE: line.slice(188, 201),
            INDOPC: line.slice(201, 202),
            DATVEN: line.slice(202, 210),
            FATCOT: line.slice(210, 217),
            PTOEXE: line.slice(217, 230),
            CODISI: line.slice(230, 242),
            DISMES: line.slice(242, 245)
        };

        const preparedData = {};        
        Object.keys(data).map(key => {
            if (formatMap[key]) preparedData[key] = null;

            if (['PREABE', 'PREMIN', 'PREMAX', 'PREMED', 'PREULT', 'PREOFC', 'PREOFV', 'PREEXE', 'PTOEXE', 'VOLTOT'].includes(key)) {
                const v = data[key];
                data[key] = v.slice(0, v.length - 2) + '.' + v.slice(v.length - 2, v.length);
            } else if (key == 'PRAZOT') {
                data['PRAZOT'] = data['PRAZOT'].replace(/[^0-9]+/g, '');
            }

            if (typeof data[key] === 'string') data[key] = data[key].trim();

            if (formatMap[key]) preparedData[key] = formatMap[key](data[key]);
            else preparedData[key] = data[key] ? data[key].replace(QUOTE, '') : null;
        });

        return preparedData;
    };

    const b3TransformerStream = new stream.Transform({
        objectMode: true,
        highWaterMark: CONFIG.highWaterMark,
        transform: (chunk, e, callback) => {
            try {
                callback(null, b3LineParser(chunk));
            } catch (ex) {
                console.error('Error: b3TransformerStream: transform');
                console.error(ex.stack);
            }
        }
    });

    return [new LineStream(), b3TransformerStream];
};

module.exports = createB3ParserAndTransformerStream;