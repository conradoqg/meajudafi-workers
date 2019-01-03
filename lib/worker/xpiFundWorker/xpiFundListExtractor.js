class XPIFundListExtractor {
    constructor(browser) {
        this.browser = browser;
    }

    async extract() {
        const page = await this.browser.newPage();
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36');
        await page.goto('https://www.xpi.com.br/investimentos/fundos-de-investimento/lista-de-fundos-de-investimento.aspx', { waitUntil: ['load', 'networkidle2'], timeout: 0 });

        const tabs = [{
            table: '#tableReferenciado',
            title: 'Internacional'
        }, {
            table: '#tableRendaFixa',
            title: 'Renda Fixa'
        }, {
            table: '#tableMultimercados',
            title: 'Multimercados'
        }, {
            table: '#tableAcoes',
            title: 'Ações'
        }, {
            table: '#tableCambial',
            title: 'Cambial'
        }];

        let header = null;
        let rows = [];

        for (const index in tabs) {
            const tab = tabs[index];

            header = await page.evaluate((tab) => {
                let tr = document.querySelectorAll(`${tab.table} > thead > tr > th`);
                let header = [];

                var cleaner = (text) => text.replace(/\n/g, '').replace(/\s{2,}/g, ' ').trim();

                tr.forEach(th => header.push(cleaner(th.innerText)));
                header.push('Início do fundo');
                header.push('Patrimônio Líquido');
                header.push('PL Médio 12M');
                header.push('Data da cota');
                header.push('Valor da cota');
                header.push('Taxa Adm.(%)');
                header.push('Taxa Perf.(%)');
                header.push('Benchmark');
                header.push('Tipo');
                header.push('CNPJ');
                return header;
            }, tab);

            rows = await page.evaluate((data, tab) => {
                console.dir(data);
                var trs = document.querySelectorAll(`${tab.table} > tbody > tr`);

                var cleaner = (text) => text.replace(/\n/g, '').replace(/\s{2,}/g, ' ').trim();

                trs.forEach(tr => {
                    const tds = tr.querySelectorAll('td');
                    let dataRow = [];
                    tds.forEach(td => {
                        const starCount = td.querySelectorAll('.icon-star').length;
                        const buttonCount = td.querySelectorAll('button').length;
                        if (starCount > 0) dataRow.push(starCount);
                        else if (buttonCount > 0) dataRow.push(cleaner(td.children[0].attributes['data-codigo'].nodeValue));
                        else if (td.innerText.trim() != '') dataRow.push(cleaner(td.innerText));
                        else dataRow.push(cleaner(td.textContent));
                    });

                    const attributes = [
                        'data-inicio',
                        'data-patrimonio',
                        'data-patrimonio12',
                        'data-cota',
                        'data-valor',
                        'data-taxa',
                        'data-taxa-performance',
                        'data-benchmark'
                    ];

                    attributes.forEach(attribute => dataRow.push(tr.attributes[attribute].nodeValue.trim()));
                    dataRow.push(tab.title);

                    data.push(dataRow);
                });
                return data;
            }, rows, tab);
        }        

        await page.close();
        
        return {
            header,
            rows
        };
    }
}

module.exports = XPIFundListExtractor;