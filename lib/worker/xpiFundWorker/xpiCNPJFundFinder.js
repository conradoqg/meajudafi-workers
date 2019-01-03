const got = require('got');
const stringSimilarity = require('string-similarity');

let fundsList = got.get('https://assets-comparacaodefundos.s3-sa-east-1.amazonaws.com/cvm/fundos').then(result => JSON.parse(result.body));

class XPICNPJFundFinder {
    constructor(browser) {
        this.browser = browser;
    }

    async extract(xpiID, fundName) {
        const page = await this.browser.newPage();
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36');
        await page.goto(`https://www.xpi.com.br/investimentos/fundos-de-investimento/detalhes-de-fundos-de-investimento.aspx?F=${xpiID}`, { waitUntil: ['load', 'networkidle2'], timeout: 0 });

        let cnpj = await page.evaluate(() => {
            let item = document.querySelector('#lnkCaracteristica') || document.querySelector('#lnkRegulamento');
            return item ? (item.href.match(/[0-9]+/g) ? item.href.match(/[0-9]+/g)[0] : '') : '';
        });

        if (cnpj.length != 14) {
            fundsList = await fundsList;

            //console.log(`Searching ${fundName} CNPJ's on google `);
            const normalizedFundName = fundName
                .replace(/(^| )(FIRF)( |$)/ig, '$1FUNDOS DE INVESTIMENTO RENDA FIXA$3')
                .replace(/(^| )(FIC)( |$)/ig, '$1FUNDOS DE INVESTIMENTO EM COTAS$3')
                .replace(/(^| )(FIM)( |$)/ig, '$1FUNDOS DE INVESTIMENTO MULTIMERCADO$3')
                .replace(/(^| )(FIA)( |$)/ig, '$1FUNDOS DE INVESTIMENTO DE AÇÕES$3')
                .replace(/(^| )(FI)( |$)/ig, '$1FUNDO DE INVESTIMENTO$3')
                .replace(/(^| )(RF)( |$)/ig, '$1RENDA FIXA$3')
                .replace(/(^| )(CP)( |$)/ig, '$1CRÉDITO PRIVADO$3')
                .replace(/(^| )(-)( |$)/ig, '$1$3')
                .replace(/(^| )(IE)( | $)/ig, '$1COM INVESTIMENTO NO EXTERIOR$3')
                .replace(/(^| )(LP)( | $)/ig, '$1LONGO PRAZO$3');

            await page.goto('https://www.google.com', { waitUntil: ['load', 'networkidle2'], timeout: 0 });
            await page.type('.gsfi', `${normalizedFundName} CNPJ`);
            await Promise.all([
                page.waitForNavigation({ waitUntil: 'networkidle2' }),
                page.keyboard.press('Enter')
            ]);

            let innerText = await page.evaluate(() => {
                return document.querySelector('body').innerText;
            });

            if (await page.$('#nav > tbody > tr > td:nth-child(3) > a') != null) {
                await Promise.all([
                    page.waitForNavigation({ waitUntil: 'networkidle2' }),
                    page.click('#nav > tbody > tr > td:nth-child(3) > a')
                ]);

                innerText += await page.evaluate(() => {
                    return document.querySelector('body').innerText;
                });
            }

            const regexp = /\d{2}\.\d{3}\.\d{3}\/\d{4}-\d{2}/g;

            const foundCNPJs = innerText.match(regexp);
            if (foundCNPJs.length > 0) {
                const cnpjFundsFound = fundsList.filter(fund => foundCNPJs.find(cnpj => fund.c == cnpj.replace(/\.|\/|-/g, '')));
                const fundNames = cnpjFundsFound.map(item => item.n);

                const bestMatch = stringSimilarity.findBestMatch(normalizedFundName, fundNames).bestMatch;
                const bestFundMatch = cnpjFundsFound.find(fund => fund.n == bestMatch.target);
                //console.log(`Fund ${fundName} (normalized as ${normalizedFundName}) found with rating ${bestMatch.rating.toFixed(2)}) as ${bestMatch.target}, ${bestMatch.rating >= 0.80 ? 'accepting' : 'ignoring'}`);
                if (bestMatch.rating >= 0.80) cnpj = bestFundMatch.c;
            }
        }
        await page.close();
        if (cnpj.length == 14) return cnpj;
    }
}

module.exports = XPICNPJFundFinder;