# 🐔 aGOrinha 2025

Implementação da Rinha de Backend 2025 utilizando **Go** com `fasthttp` e processamento assíncrono.
Repositório do projeto: [https://github.com/willy-r/aGOrinha-2025](https://github.com/willy-r/aGOrinha-2025)

## 🔧 Tecnologias utilizadas

- **Linguagem**: Go 1.24
- **Servidor HTTP**: [fasthttp](https://github.com/valyala/fasthttp)
- **Persistência**: Em memória, arquivos `.json` com `flock`
- **Orquestração**: Docker + Docker Compose
- **Load Balancer**: NGINX

## 🧠 Estratégia

- Pool de workers para processamento paralelo
- Armazenamento eficiente com mínima sobrecarga
