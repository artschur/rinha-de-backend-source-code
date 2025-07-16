# 🔥🍞 Rinha de Backend 2025 - Bun 🍞🔥

## **Stack**
• Language / Runtime: Bun (TypeScript)  
• Storage: MongoDB  
• Queue: Redis  
• Load-balancer / Edge: Traefik  
• Orchestration: Docker Compose

## **Run it**
1. Start the payment-processor compose first (per its repo).
2. `docker compose up --build` in this repo.

## **What it does**  
Two Bun API instances and a worker talk over Redis to process payments asynchronously, persisting everything in MongoDB. Traefik handles routing, and fail-over between default and fallback payment processors is automatic.

Repo: https://github.com/ryangst/rinha-backend-2025-codend-2025](https://github.com/LuizCordista/rinha-backend-2025)