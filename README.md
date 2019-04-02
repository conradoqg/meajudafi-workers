# meajudafi-workers

Background workers for the [meajudafi](https://github.com/conradoqg/meajudafi-stack) stack.

## Available workers

- cvmDataWorker: Download, extract and insert CVM funds list, indicators data into database;
- cvmStatisticWorker: Calculate a wide range of statistics of the CVM funds including investment return, sharpe, consistency, risk, and so on;
- dataImprovementWorker: Improves and unify data gathered from the sources, does a great job on funds name's and non oficial data like initial investment, quota limits and so on;
- xpiFundWorker: Download, cleanup and extract data from XPI broker;
- btgPactualFundWorker: Download, cleanup and extract data from BTG Pactual broker;
- migrateWorker: Migrates the database;
- all: Run all the above workers except `migrateWorker`;

## Usage

```sh
$ ./bin/cvmfe.js run <worker> <options>
```

## Related repositories

- [meajudafi-front-end](https://github.com/conradoqg/meajudafi-front-end)
- [meajudafi-stack](https://github.com/conradoqg/meajudafi-stack)
- [meajudafi-docker-container-crontab](https://github.com/conradoqg/meajudafi-docker-container-crontab)

License
----
This project is licensed under the [MIT](LICENSE.md) License.