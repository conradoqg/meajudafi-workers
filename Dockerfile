FROM node:8-alpine

RUN apk update && apk upgrade && \
    echo @edge http://nl.alpinelinux.org/alpine/v3.8/community >> /etc/apk/repositories && \
    echo @edge http://nl.alpinelinux.org/alpine/v3.8/main >> /etc/apk/repositories && \
    apk add --no-cache \
      chromium@edge \
      nss@edge

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD true

RUN mkdir /cvm-fund-explorer-workers

COPY . /cvm-fund-explorer-workers

WORKDIR /cvm-fund-explorer-workers

VOLUME /cvm-fund-explorer-workers/db

RUN npm install --only=production

RUN addgroup -S pptruser && adduser -S -g pptruser pptruser \
    && mkdir -p /home/pptruser/Downloads \
    && chown -R pptruser:pptruser /home/pptruser \
    && chown -R pptruser:pptruser /cvm-fund-explorer-workers

USER pptruser

ENTRYPOINT [ "node", "--inspect=0.0.0.0", "./bin/cvmfe.js" ]