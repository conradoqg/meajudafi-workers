const csv = require('fast-csv');

const QUOTE = /'/g;

const createCSVParserAndTransformerStream = (formatMap, delimiter = ';') => {
    let header = null;

    return csv({ delimiter, includeEndRowDelimiter: true })
        .transform((data) => {
            try {
                if (header == null) header = data;
                else {
                    const preparedData = {};
                    Object.keys(formatMap).map(key => preparedData[key] = null);
                    data.map((item, index) => {
                        if (formatMap[header[index]]) preparedData[header[index]] = formatMap[header[index]](item);
                        else preparedData[header[index]] = item ? item.replace(QUOTE, '') : null;
                    });

                    return preparedData;
                }
            } catch (ex) {
                console.error('Error: createCSVParserAndTransformerStream: transform');
                console.error(ex.stack);
            }
        });
};

module.exports = createCSVParserAndTransformerStream;