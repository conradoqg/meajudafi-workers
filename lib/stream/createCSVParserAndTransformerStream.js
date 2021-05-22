const csv = require('fast-csv');

const QUOTE = /'/g;

const createCSVParserAndTransformerStream = (formatMap, delimiter = ';', defaultData, validLine, quote = null) => {
    let header = null;

    return csv({ delimiter, includeEndRowDelimiter: true, quote })
        .validate(data => validLine ? validLine(data) : true)
        .transform((data) => {
            try {
                if (header == null) header = data.map(col => col.replace(/"/g, ''));
                else {                    
                    const preparedData = {};
                    defaultData && Object.keys(defaultData).map(key => preparedData[key] = defaultData[key]);
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