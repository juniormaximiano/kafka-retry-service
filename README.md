# 🔁 Kafka Retry Service

![Java](https://img.shields.io/badge/Java-25-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.16-brightgreen)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9-black)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Status](https://img.shields.io/badge/Status-POC-success)

POC desenvolvida para demonstrar o processamento assíncrono de requisições utilizando **Spring Boot**, **Apache Kafka** e **Spring Retry**.

A aplicação recebe requisições através de uma API REST, cria um identificador único para cada solicitação, mantém seu estado em memória, publica um evento no Kafka e processa a mensagem através de um Consumer.

Caso o processamento falhe, o método utiliza `@Retryable` para realizar novas tentativas automaticamente. Se todas as tentativas forem esgotadas, o `@Recover` atualiza a requisição para o estado `FAILED`.

---

# 🎯 Objetivo da POC

O objetivo principal é demonstrar um fluxo semelhante ao seguinte cenário:

```text
Sistema de origem
        ↓
     API REST
        ↓
 Criação da Request
        ↓
 Armazenamento em memória
        ↓
   Kafka Producer
        ↓
       Kafka
        ↓
   Kafka Consumer
        ↓
 ProcessingService
        ↓
    @Retryable
        ↓
 ┌───────────────┐
 │               │
Sucesso        Falha
 │               │
 ▼               ▼
SUCCESSFUL   Nova tentativa
                 │
                 ▼
           Limite atingido
                 │
                 ▼
              @Recover
                 │
                 ▼
               FAILED
```

A POC simula um cenário em que um sistema recebe dados relacionados a exames médicos e precisa encaminhá-los para um processamento externo.

A integração final real não faz parte desta POC.

O foco está em demonstrar:

* recebimento das requisições;
* processamento assíncrono;
* desacoplamento com Kafka;
* controle de status;
* novas tentativas automáticas;
* tratamento de falha definitiva.

---

# 🧠 Problema resolvido

Sem mensageria e sem retry, uma falha temporária poderia interromper completamente o fluxo:

```text
Sistema envia requisição
        ↓
Processamento falha
        ↓
      ERRO
        ↓
      FIM
```

Com a arquitetura da POC:

```text
Sistema envia requisição
        ↓
API recebe e registra
        ↓
Kafka recebe o evento
        ↓
Consumer processa
        ↓
Falhou?
        ↓
Tenta novamente
        ↓
Sucesso ou falha definitiva
```

Assim, o sistema de origem não precisa esperar todo o processamento ser concluído durante a requisição HTTP.

---

# 🏗️ Arquitetura

```text
                         ┌───────────────────┐
                         │   Sistema origem  │
                         └─────────┬─────────┘
                                   │
                              HTTP POST
                                   │
                                   ▼
                         ┌───────────────────┐
                         │ RequestController │
                         └─────────┬─────────┘
                                   │
                                   ▼
                         ┌───────────────────┐
                         │  RequestService   │
                         └──────┬──────┬─────┘
                                │      │
                                │      ▼
                                │  RequestStore
                                │
                                ▼
                         ┌───────────────────┐
                         │ RequestProducer   │
                         └─────────┬─────────┘
                                   │
                                   ▼
                         ╔═══════════════════╗
                         ║   Apache Kafka    ║
                         ║                   ║
                         ║ request-created- ║
                         ║      topic        ║
                         ╚═════════╤═════════╝
                                   │
                                   ▼
                         ┌───────────────────┐
                         │ RequestConsumer   │
                         └─────────┬─────────┘
                                   │
                                   ▼
                         ┌───────────────────┐
                         │ ProcessingService │
                         │                   │
                         │    @Retryable     │
                         │    @Recover       │
                         └─────────┬─────────┘
                                   │
                         ┌─────────┴──────────┐
                         │                    │
                         ▼                    ▼
                    SUCCESSFUL              FAILED
```

---

# ⚙️ Tecnologias utilizadas

## Backend

* Java
* Spring Boot
* Spring Web
* Bean Validation
* Spring Retry
* Spring AOP
* Spring for Apache Kafka

## Infraestrutura

* Apache Kafka
* Docker
* Docker Compose

## Documentação e testes

* SpringDoc OpenAPI
* Swagger UI

---

# 📁 Estrutura do projeto

```text
src/main/java/com/juno/kafkaretryservice
│
├── consumer
│   └── RequestConsumer.java
│
├── controller
│   └── RequestController.java
│
├── domain
│   ├── Request.java
│   └── RequestStatus.java
│
├── dto
│   └── RequestCreateDTO.java
│
├── event
│   └── RequestCreatedEvent.java
│
├── producer
│   └── RequestProducer.java
│
├── service
│   ├── RequestService.java
│   └── ProcessingService.java
│
└── store
    └── RequestStore.java
```

---

# 🔄 Fluxo completo da aplicação

## 1️⃣ A API recebe a requisição

O sistema recebe os dados através de:

```http
POST /requests
```

Exemplo:

```json
{
  "accessionNumber": "2616003357",
  "studyDate": "2026-06-09T16:02:00",
  "studyDescription": "RX PE ESQUERDO AP E OBLIQUO",
  "modality": "DX",
  "patientId": "4413400014",
  "patientName": "PACIENTE TESTE",
  "patientBirthDate": "1944-05-13",
  "patientSex": "F",
  "reportPdfBase64": "",
  "simulateFailure": false
}
```

---

## 2️⃣ A Request é criada

Cada requisição recebe automaticamente:

```text
UUID
Status inicial
Número de tentativas
Data de criação
Data de atualização
```

Exemplo:

```text
id        → UUID gerado automaticamente

status    → PENDING

attempts  → 0

createdAt → momento da criação

updatedAt → última atualização
```

---

## 3️⃣ A requisição é armazenada em memória

A POC não utiliza banco de dados.

As requisições são armazenadas em:

```java
ConcurrentHashMap<UUID, Request>
```

A estrutura funciona como:

```text
UUID → Request
```

Exemplo:

```text
550e8400... → Request(PENDING)

8192ee5c... → Request(PROCESSING)

a45729db... → Request(SUCCESSFUL)
```

O `ConcurrentHashMap` foi escolhido porque diferentes fluxos podem acessar as requisições:

```text
API REST
   ↓
cria e consulta requests


Kafka Consumer
   ↓
busca e atualiza requests
```

Como o processamento é assíncrono, essa estrutura é mais adequada que um `HashMap` comum para acessos concorrentes.

> Em um ambiente de produção, o armazenamento em memória poderia ser substituído por uma camada de persistência.

---

# 📤 Kafka Producer

Após a criação da Request, a aplicação cria um evento:

```text
RequestCreatedEvent
```

O evento contém informações suficientes para identificar a requisição:

```text
requestId
accessionNumber
patientId
```

O Producer publica esse evento no tópico:

```text
request-created-topic
```

Fluxo:

```text
RequestService
      ↓
RequestProducer
      ↓
KafkaTemplate
      ↓
request-created-topic
```

O `KafkaTemplate` é a ferramenta disponibilizada pelo Spring para publicar mensagens no Kafka.

---

# 📥 Kafka Consumer

O Consumer permanece escutando o tópico:

```text
request-created-topic
```

Quando uma mensagem chega:

```text
Kafka
   ↓
RequestConsumer
   ↓
ProcessingService
```

O Consumer possui uma responsabilidade simples:

> receber a mensagem do Kafka e delegar o processamento.

A regra de processamento não fica dentro do Consumer.

Essa separação mantém as responsabilidades claras:

```text
Producer
→ publica

Consumer
→ recebe

ProcessingService
→ processa
```

---

# 🔁 Retry automático

O método responsável pelo processamento utiliza:

```java
@Retryable
```

Configuração da POC:

```java
@Retryable(
    maxAttempts = 3,
    retryFor = RuntimeException.class,
    backoff = @Backoff(delay = 10000)
)
```

Isso significa:

```text
Máximo de tentativas: 3

Exceção que gera retry:
RuntimeException

Intervalo entre tentativas:
10 segundos
```

O retry só acontece quando ocorre uma exceção.

---

# ✅ Cenário de sucesso

Quando não ocorre nenhuma falha:

```text
Tentativa 1
    ↓
Processamento concluído
    ↓
SUCCESSFUL
```

Resultado:

```json
{
  "status": "SUCCESSFUL",
  "attempts": 1
}
```

---

# ❌ Cenário de falha

Quando a falha é simulada:

```text
Tentativa 1
    ↓
ERRO
    ↓
espera 10 segundos

Tentativa 2
    ↓
ERRO
    ↓
espera 10 segundos

Tentativa 3
    ↓
ERRO
    ↓
@Recover
    ↓
FAILED
```

Resultado:

```json
{
  "status": "FAILED",
  "attempts": 3
}
```

---

# 🛟 Recuperação com `@Recover`

Quando todas as tentativas configuradas no `@Retryable` são esgotadas, o Spring executa:

```java
@Recover
```

Nesse momento:

```text
Request é localizada pelo UUID
        ↓
status = FAILED
        ↓
updatedAt é atualizado
```

Isso representa a falha definitiva do processamento.

---

# 🧪 Simulação de falha

A POC possui o campo:

```json
"simulateFailure": true
```

Ele existe exclusivamente para demonstrar o comportamento do retry.

## Processamento normal

```json
"simulateFailure": false
```

Resultado:

```text
Tentativa 1
↓
SUCCESSFUL
```

## Processamento com falha

```json
"simulateFailure": true
```

Resultado:

```text
Tentativa 1
↓
erro

Tentativa 2
↓
erro

Tentativa 3
↓
erro

@Recover
↓
FAILED
```

Em uma integração real, a exceção poderia ser causada por:

* timeout;
* sistema externo indisponível;
* falha de comunicação;
* erro temporário de integração;
* indisponibilidade de serviço.

---

# 📦 Processamento em lote

A aplicação possui um endpoint para envio de múltiplas requisições:

```http
POST /requests/batch
```

Exemplo:

```json
[
  {
    "accessionNumber": "10001",
    "studyDate": "2026-07-09T20:55:51",
    "studyDescription": "TC DE TORAX",
    "modality": "CT",
    "patientId": "P001",
    "patientName": "PACIENTE TESTE 01",
    "patientBirthDate": "1990-01-01",
    "patientSex": "M",
    "reportPdfBase64": "",
    "simulateFailure": false
  },
  {
    "accessionNumber": "10002",
    "studyDate": "2026-07-09T20:55:51",
    "studyDescription": "TESTE DE RETRY",
    "modality": "CT",
    "patientId": "P002",
    "patientName": "PACIENTE TESTE RETRY",
    "patientBirthDate": "1992-02-02",
    "patientSex": "F",
    "reportPdfBase64": "",
    "simulateFailure": true
  }
]
```

Cada item:

```text
gera uma Request
        ↓
gera um UUID
        ↓
é armazenado em memória
        ↓
gera um evento
        ↓
é publicado no Kafka
```

Isso permite testar múltiplas requisições no mesmo fluxo.

---

# 📊 Estados da Request

A Request possui quatro possíveis estados:

## 🟡 `PENDING`

A requisição foi criada e ainda aguarda processamento.

```text
API recebeu
↓
Request criada
↓
PENDING
```

---

## 🔵 `PROCESSING`

O Consumer recebeu o evento e iniciou o processamento.

```text
Kafka
↓
Consumer
↓
ProcessingService
↓
PROCESSING
```

---

## 🟢 `SUCCESSFUL`

O processamento foi concluído.

```text
Processamento
↓
sucesso
↓
SUCCESSFUL
```

---

## 🔴 `FAILED`

Todas as tentativas falharam.

```text
Tentativa 1 → erro

Tentativa 2 → erro

Tentativa 3 → erro

@Recover

FAILED
```

---

# 🌐 Endpoints

## Criar uma requisição

```http
POST /requests
```

---

## Criar requisições em lote

```http
POST /requests/batch
```

---

## Listar todas as requisições

```http
GET /requests
```

---

## Buscar uma requisição por UUID

```http
GET /requests/{id}
```

---

# 🐳 Executando o Kafka

O Kafka é executado através do Docker Compose.

Subir os serviços:

```bash
docker compose up -d
```

Verificar os containers:

```bash
docker compose ps
```

Parar os serviços:

```bash
docker compose down
```

---

# ▶️ Executando a aplicação

## Windows

```bash
.\mvnw spring-boot:run
```

## Linux/macOS

```bash
./mvnw spring-boot:run
```

---

# 📖 Swagger

Com a aplicação em execução:

```text
http://localhost:8080/swagger-ui/index.html
```

No Swagger é possível:

* criar uma requisição;
* criar requisições em lote;
* listar requisições;
* consultar por UUID;
* simular sucesso;
* simular falha;
* acompanhar tentativas.

---

# 🖥️ Logs da aplicação

Os logs foram mantidos simples para facilitar a demonstração da POC.

Exemplo de sucesso:

```text
>> Requisição 10001 | Tentativa: 1 | Falha simulada: false
```

Resultado:

```text
SUCCESSFUL
```

Exemplo de retry:

```text
>> Requisição 10009 | Tentativa: 1 | Falha simulada: true

>> Requisição 10009 | Tentativa: 2 | Falha simulada: true

>> Requisição 10009 | Tentativa: 3 | Falha simulada: true
```

Resultado:

```text
Processamento falhou definitivamente

FAILED
```

---

# 🧩 Responsabilidades das classes

## `RequestController`

Responsável pela entrada HTTP.

```text
HTTP
↓
Controller
↓
Service
```

---

## `RequestService`

Responsável pela criação da Request e início do fluxo.

```text
Cria Request
↓
salva em memória
↓
cria evento
↓
chama Producer
```

---

## `RequestStore`

Responsável pelo armazenamento em memória.

```text
UUID → Request
```

---

## `RequestProducer`

Responsável por publicar mensagens no Kafka.

---

## `RequestConsumer`

Responsável por consumir mensagens do Kafka.

---

## `ProcessingService`

Responsável pelo processamento.

Contém:

```text
@Retryable
@Recover
```

---

# 🚀 Exemplo de fluxo completo

```text
1. Cliente envia POST /requests

2. Controller recebe o JSON

3. DTO representa os dados recebidos

4. Service cria a Request

5. UUID é gerado

6. Status inicial = PENDING

7. Request é armazenada no ConcurrentHashMap

8. Evento é criado

9. Producer publica no Kafka

10. Consumer recebe o evento

11. ProcessingService busca a Request pelo UUID

12. Status = PROCESSING

13. Tentativa é incrementada

14. Processamento é executado

15A. Sucesso:
     SUCCESSFUL

15B. Falha:
     @Retryable executa novamente

16. Tentativas esgotadas:
    @Recover

17. Status = FAILED
```

---

# 🔮 Possíveis evoluções

Por se tratar de uma POC, alguns recursos foram mantidos fora do escopo atual.

Possíveis evoluções:

* persistência em banco de dados;
* Dead Letter Topic (DLT);
* métricas com Actuator;
* Prometheus e Grafana;
* logs estruturados;
* configuração externa do número de tentativas;
* configuração externa do backoff;
* integração real com sistema de destino;
* testes automatizados;
* tratamento específico de exceções;
* múltiplos Consumers;
* particionamento do tópico Kafka.

---

# 💡 Decisões técnicas

## Por que Kafka?

Para desacoplar o recebimento da requisição do processamento.

```text
API recebe
↓
Kafka mantém a mensagem disponível
↓
Consumer processa separadamente
```

---

## Por que `ConcurrentHashMap`?

Porque a POC não exige banco de dados e existe acesso assíncrono às requisições.

```text
API acessa
Consumer acessa
ProcessingService atualiza
```

---

## Por que `@Retryable`?

Para tratar automaticamente falhas temporárias.

Sem retry:

```text
Erro
↓
fim
```

Com retry:

```text
Erro
↓
espera
↓
nova tentativa
```

---

## Por que `@Recover`?

Para tratar o cenário em que todas as tentativas foram esgotadas.

```text
Retries esgotados
↓
@Recover
↓
FAILED
```

---

## Por que não usar banco de dados?

O objetivo da POC é demonstrar:

```text
API
↓
Kafka
↓
Consumer
↓
Retry
↓
Resultado
```

Adicionar persistência não era necessário para validar esse fluxo.

Em produção, o armazenamento em memória poderia ser substituído por uma solução persistente.

---

# ✅ Status da POC

* ✅ API REST funcionando.
* ✅ Payload baseado no cenário real.
* ✅ Requests com UUID.
* ✅ Status controlados.
* ✅ Armazenamento em memória.
* ✅ Consulta das requisições.
* ✅ Kafka executando via Docker.
* ✅ Producer publicando eventos.
* ✅ Consumer consumindo eventos.
* ✅ Processamento assíncrono.
* ✅ `@Retryable` funcionando.
* ✅ Backoff entre tentativas.
* ✅ `@Recover` funcionando.
* ✅ Cenário de sucesso.
* ✅ Cenário de falha definitiva.
* ✅ Processamento em lote.
* ✅ Simulação controlada de falhas.
* ✅ Swagger para demonstração.

---

# 👨‍💻 Autor

Desenvolvido por **Adeilson Maximiano Junior** como uma POC de processamento assíncrono utilizando Apache Kafka e Spring Retry.