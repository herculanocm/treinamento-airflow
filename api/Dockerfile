# API Node.js/Express do treinamento (ver server.js)
FROM node:22-alpine

WORKDIR /app

# Instala as dependências antes de copiar o código para aproveitar o cache
COPY package*.json ./
RUN npm install --omit=dev

COPY server.js ./

EXPOSE 3000
USER node
CMD ["node", "server.js"]
