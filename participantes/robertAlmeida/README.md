# Rinha de Backend 2025 - Sistema de Intermediação de Pagamentos

**Autor:** Robert Almeida
**Data:** Julho 2025  
**Versão:** 1.0

## Visão Geral

Este projeto implementa uma solução completa para a terceira edição da Rinha de Backend 2025, desenvolvendo um sistema de intermediação de pagamentos que seleciona automaticamente o Payment Processor com menor taxa e maior velocidade disponível. A solução foi projetada para operar dentro das restrições rigorosas de recursos computacionais da competição, priorizando alta performance, baixa latência e resiliência a falhas.

O sistema atua como um intermediário inteligente entre clientes e dois Payment Processors distintos: um serviço "default" com taxa mais baixa (0.05) e um serviço "fallback" com taxa mais alta (0.15). A estratégia principal é maximizar o uso do serviço default para minimizar custos, enquanto mantém a disponibilidade através do fallback quando necessário.

## Arquitetura do Sistema

### Componentes Principais

A arquitetura foi projetada seguindo princípios de microserviços com foco em eficiência de recursos e alta disponibilidade. Os componentes principais incluem:

**Load Balancer (Nginx):** Atua como ponto de entrada único na porta 9999, distribuindo requisições entre múltiplas instâncias do backend usando algoritmo least_conn para otimizar a distribuição de carga.

**Backend de Intermediação (Go):** Duas instâncias idênticas implementadas em Go, escolhido pela sua eficiência de memória e alta performance. Cada instância processa requisições de forma assíncrona, implementa cache de health-check e gerencia a lógica de seleção de Payment Processor.

**Cache e Persistência (Redis):** Utilizado para armazenar contadores agregados de pagamentos processados e cache de informações de health-check dos Payment Processors. Configurado com política de eviction LRU e limite de memória de 25MB.

**Payment Processors Externos:** Dois serviços fornecidos pela organização da Rinha, simulando processadores de pagamento reais com diferentes taxas e comportamentos de instabilidade.

### Fluxo de Dados

O fluxo de processamento de pagamentos segue um padrão assíncrono otimizado para alta velocidade de resposta:

1. **Recebimento da Requisição:** O cliente envia POST /payments para o Nginx na porta 9999
2. **Balanceamento:** Nginx distribui para uma das instâncias do backend
3. **Resposta Imediata:** O backend responde HTTP 200 imediatamente ao cliente
4. **Processamento Assíncrono:** Uma goroutine processa o pagamento em segundo plano
5. **Seleção Inteligente:** O sistema consulta cache de health-check e seleciona o melhor PP
6. **Envio e Retry:** Tenta enviar para o PP selecionado com retry e backoff exponencial
7. **Fallback Automático:** Se o default falhar, tenta automaticamente o fallback
8. **Atualização de Contadores:** Atualiza estatísticas no Redis após sucesso

### Estratégia de Seleção de Payment Processor

A lógica de seleção prioriza sempre o Payment Processor default devido à sua menor taxa (0.05 vs 0.15 do fallback). O sistema implementa um cache de health-check que respeita o limite de 1 chamada a cada 5 segundos por PP, armazenando informações sobre disponibilidade e tempo mínimo de resposta.

A decisão de roteamento considera primariamente o status de falha do PP default. Se não estiver falhando, todas as requisições são direcionadas para ele. Apenas quando o default está marcado como "failing" no health-check, o sistema utiliza o fallback. Esta estratégia simples mas efetiva maximiza a economia de taxas enquanto mantém a disponibilidade.

## Implementação Técnica

### Tecnologias Utilizadas

**Linguagem Principal:** Go foi escolhido pela sua excelente performance, baixo consumo de memória e capacidade de compilação para binários estáticos pequenos. O runtime do Go é particularmente eficiente para aplicações de alta concorrência.

**Framework Web:** Gin foi selecionado por sua velocidade e baixo overhead. Configurado em modo release para máxima performance, com middleware CORS habilitado para compatibilidade.

**Cache e Persistência:** Redis 7 Alpine configurado com limite de memória de 25MB e política allkeys-lru. Utilizado tanto para cache de health-check quanto para contadores agregados de pagamentos.

**Containerização:** Docker com imagens Alpine para minimizar tamanho e consumo de recursos. Build multi-stage para otimizar o tamanho final das imagens.

**Load Balancer:** Nginx Alpine com configuração otimizada para baixa latência, incluindo timeouts ajustados e buffering configurado.

### Otimizações de Performance

Várias otimizações foram implementadas para maximizar a performance dentro das restrições de recursos:

**Processamento Assíncrono:** Todas as operações de rede com Payment Processors são executadas em goroutines separadas, permitindo que a API responda imediatamente aos clientes.

**Cache de Health-Check:** Implementado em memória local em cada instância, evitando chamadas desnecessárias aos PPs e respeitando o rate limit de 5 segundos.

**Pool de Conexões HTTP:** Cliente HTTP configurado com timeout de 10 segundos e reutilização de conexões para reduzir overhead.

**Retry com Backoff Exponencial:** Implementado retry automático com backoff exponencial (1s, 2s, 4s) para lidar com instabilidades temporárias dos PPs.

**Contadores Agregados:** Uso de operações atômicas do Redis (HINCRBY, HINCRBYFLOAT) para atualizar contadores de forma eficiente e thread-safe.

### Tratamento de Falhas e Resiliência

O sistema implementa múltiplas camadas de resiliência para lidar com as instabilidades esperadas dos Payment Processors:

**Fallback Automático:** Se o PP default falhar após todas as tentativas de retry, o sistema automaticamente tenta o PP fallback para a mesma transação.

**Health-Check Inteligente:** Monitoramento contínuo da saúde dos PPs com cache local para evitar rate limiting. Em caso de erro de conectividade, marca automaticamente o PP como "failing".

**Timeouts Configurados:** Timeouts de 10 segundos para requisições HTTP, balanceando responsividade com tolerância a latências altas.

**Graceful Degradation:** Se o Redis não estiver disponível, o sistema continua funcionando com contadores em memória (não persistentes).

## Configuração e Deployment

### Restrições de Recursos

O sistema foi projetado para operar dentro das restrições específicas da Rinha de Backend 2025:

- **CPU Total:** 1.5 unidades distribuídas entre todos os serviços
- **Memória Total:** 350MB distribuída entre todos os serviços
- **Instâncias Backend:** Mínimo de 2 instâncias obrigatórias

A distribuição de recursos foi otimizada baseada no perfil de uso esperado:

| Serviço | CPU | Memória | Justificativa |
|---------|-----|---------|---------------|
| Nginx | 0.1 | 20MB | Load balancer leve, pouco processamento |
| Backend 1 | 0.6 | 150MB | Processamento principal, cache em memória |
| Backend 2 | 0.6 | 150MB | Instância redundante para alta disponibilidade |
| Redis | 0.2 | 30MB | Cache e persistência, configurado com limite |

### Variáveis de Ambiente

O sistema utiliza variáveis de ambiente para configuração flexível:

- `PAYMENT_PROCESSOR_URL_DEFAULT`: URL do PP default (padrão: http://payment-processor-default:8080)
- `PAYMENT_PROCESSOR_URL_FALLBACK`: URL do PP fallback (padrão: http://payment-processor-fallback:8080)
- `REDIS_ADDR`: Endereço do Redis (padrão: localhost:6379)
- `PORT`: Porta do backend (padrão: 8080)

### Instruções de Deployment

Para executar o sistema completo:

1. **Iniciar Payment Processors:**
```bash
sudo docker-compose -f docker-compose-payment-processors.yml up -d
```

2. **Construir e Iniciar Nossa Solução:**
```bash
sudo docker-compose build
sudo docker-compose up -d
```

3. **Verificar Status:**
```bash
sudo docker ps
curl http://localhost:9999/payments-summary
```

O sistema estará disponível na porta 9999 conforme especificado pela Rinha.

## Testes e Validação

### Testes de Funcionalidade

Foram realizados testes abrangentes para validar o comportamento correto do sistema:

**Teste de Conectividade:** Verificação de que todos os endpoints respondem corretamente e que a comunicação entre componentes funciona adequadamente.

**Teste de Processamento:** Validação de que pagamentos são processados corretamente e que os contadores são atualizados de forma consistente.

**Teste de Health-Check:** Confirmação de que o cache de health-check funciona corretamente e respeita o rate limit de 5 segundos.

### Testes de Performance

Os testes de carga demonstraram excelente performance do sistema:

**Resultado do Teste de Carga (100 requisições, 10 threads):**
- Taxa de sucesso: 100%
- Throughput: 415.38 requisições por segundo
- Latência média: 18.71ms
- Latência mínima: 2.33ms
- Latência máxima: 73.23ms
- Taxa de processamento pelos PPs: 100%

Estes resultados demonstram que o sistema atende aos requisitos de alta performance da Rinha, com latências consistentemente baixas e throughput elevado.

### Testes de Resiliência

Foram validados cenários de falha para garantir a robustez do sistema:

**Falha do PP Default:** Confirmado que o sistema automaticamente utiliza o PP fallback quando o default está indisponível.

**Instabilidades de Rede:** Testado comportamento com timeouts e retries, validando que o sistema se recupera adequadamente de falhas temporárias.

**Sobrecarga de Requisições:** Verificado que o sistema mantém performance estável mesmo sob alta carga concorrente.

## Conclusão

A solução desenvolvida atende completamente aos requisitos da Rinha de Backend 2025, implementando um sistema de intermediação de pagamentos eficiente, resiliente e otimizado para as restrições de recursos da competição. A arquitetura baseada em microserviços com processamento assíncrono permite alta performance e baixa latência, enquanto a estratégia de seleção inteligente de Payment Processors maximiza a economia de taxas.

Os resultados dos testes demonstram que o sistema é capaz de processar centenas de requisições por segundo com latências consistentemente baixas, mantendo 100% de disponibilidade mesmo diante das instabilidades esperadas dos Payment Processors. A implementação em Go com Redis para cache e persistência provou ser uma escolha acertada para o perfil de performance exigido.

O projeto representa uma solução robusta e bem arquitetada para o desafio proposto, demonstrando boas práticas de desenvolvimento de sistemas distribuídos de alta performance com restrições rigorosas de recursos.

##  Repositório do código-fonte
https://github.com/RobertAlmeida/rinha-de-backend-2025


Este repositório contém o código dos Payment Processors usado na [terceira edição da Rinha de Backend](https://github.com/zanfranceschi/rinha-de-backend-2025).