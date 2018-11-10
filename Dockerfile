FROM node:8-alpine

RUN apk update && apk upgrade && \
    echo @edge http://nl.alpinelinux.org/alpine/edge/community >> /etc/apk/repositories && \
    echo @edge http://nl.alpinelinux.org/alpine/edge/main >> /etc/apk/repositories && \
    apk add --no-cache \
      chromium@edge \
      nss@edge

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD true

RUN yarn add puppeteer@1.4.0

RUN addgroup -S pptruser && adduser -S -g pptruser pptruser \
    && mkdir -p /home/pptruser/Downloads \
    && chown -R pptruser:pptruser /home/pptruser \
    && chown -R pptruser:pptruser /app

USER pptruser

RUN mkdir /cvm-fund-explorer-workers

COPY . /cvm-fund-explorer-workers

WORKDIR /cvm-fund-explorer-workers

VOLUME /cvm-fund-explorer-workers/db

RUN npm install --only=production

ENTRYPOINT [ "node", "./bin/cvmfe.js" ]