# Rinha de Backend 2025 - Submissão

## Tecnologias utilizadas

- **Linguagem:** Go
- **Armazenamento/Fila:** Redis
- **Balanceador:** Nginx
- **Orquestração:** Docker Compose

## Como rodar

1. Suba o docker-compose dos Payment Processors primeiro (conforme instruções do repositório oficial).
2. Depois, suba este compose:
   ```sh
   docker compose up --build
   ```
3. O backend ficará disponível na porta **9999**.

## Sobre a solução

O backend foi desenvolvido em Go, processando pagamentos de forma assíncrona com Redis como fila e armazenamento. O Nginx faz o balanceamento entre as instâncias. O health check dos processadores é respeitado para priorizar o default e só usar o fallback quando necessário.

## Repositório do código-fonte

[https://github.com/LuizCordista/rinha-backend-2025](https://github.com/LuizCordista/rinha-backend-2025)