FROM node:12-alpine3.9

RUN apk update && apk upgrade && \
    echo @edge http://nl.alpinelinux.org/alpine/v3.9/community >> /etc/apk/repositories && \
    echo @edge http://nl.alpinelinux.org/alpine/v3.9/main >> /etc/apk/repositories && \
    apk add --no-cache \
      chromium@edge \
      nss@edge

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD true

RUN mkdir /cvm-fund-explorer-workers

COPY . /cvm-fund-explorer-workers

WORKDIR /cvm-fund-explorer-workers

VOLUME /cvm-fund-explorer-workers/db

RUN npm install --only=production

RUN npm list | grep pupp

RUN /usr/bin/chromium-browser --version

RUN addgroup -S pptruser && adduser -S -g pptruser pptruser \
    && mkdir -p /home/pptruser/Downloads \
    && chown -R pptruser:pptruser /home/pptruser \
    && chown -R pptruser:pptruser /cvm-fund-explorer-workers

USER pptruser

ENTRYPOINT [ "node", "--inspect=0.0.0.0", "./bin/cvmfe.js" ]