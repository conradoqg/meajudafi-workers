FROM node:8-alpine

RUN mkdir /cvm-fund-explorer-workers

COPY . /cvm-fund-explorer-workers

WORKDIR /cvm-fund-explorer-workers

VOLUME /cvm-fund-explorer-workers/db

RUN npm install --only=production

ENTRYPOINT [ "node", "./bin/cvmfe.js" ]