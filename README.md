# 🔁 Kafka Retry Service

POC desenvolvida para demonstrar processamento assíncrono de requisições utilizando **Spring Boot**, **Apache Kafka**, **Spring Retry** e integração HTTP com **RestTemplate**.

A aplicação recebe requisições através de uma API REST, cria um identificador único para cada solicitação, mantém seu estado em memória, publica um evento no Kafka e processa a mensagem através de um Consumer.

Durante o processamento, a aplicação realiza um `POST` para uma API externa.

Caso a integração externa responda com erro HTTP, o `RestTemplate` propaga a falha para o fluxo de processamento. O `@Retryable` realiza novas tentativas automaticamente e, caso todas sejam esgotadas, o `@Recover` atualiza a requisição para o estado `FAILED`.

---

# 🎯 Objetivo da POC

O objetivo principal é demonstrar um fluxo de processamento assíncrono com integração externa e mecanismo automático de retry.

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
    RestTemplate
        ↓
    API Externa
        ↓
 ┌───────────────┐
 │               │
2xx           4xx / 5xx
 │               │
 ▼               ▼
SUCCESSFUL    Exceção
                 │
                 ▼
             @Retryable
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

A POC representa um cenário em que um sistema recebe dados relacionados a exames médicos e precisa encaminhá-los para um serviço externo.

A API externa utilizada na demonstração é representada por um **Postman Mock Server**.

O foco está em demonstrar:

* recebimento de requisições;
* processamento assíncrono;
* desacoplamento com Kafka;
* integração HTTP;
* envio de DTO para uma API externa;
* controle de status;
* retry automático;
* tratamento de erros HTTP;
* tratamento de falha definitiva.

---

# 🧠 Problema resolvido

Sem mensageria e sem retry, uma falha temporária em um serviço externo poderia interromper completamente o fluxo.

```text
Sistema envia requisição
        ↓
API externa indisponível
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
POST na API externa
        ↓
Falhou?
        ↓
Tenta novamente
        ↓
Sucesso ou falha definitiva
```

Assim, o sistema de origem não precisa aguardar todo o processamento durante a requisição HTTP inicial.

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
                         ║ request-created-  ║
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
                              RestTemplate
                                   │
                                   ▼
                         ┌───────────────────┐
                         │    API Externa    │
                         └─────────┬─────────┘
                                   │
                         ┌─────────┴─────────┐
                         │                   │
                         ▼                   ▼
                        2xx               4xx / 5xx
                         │                   │
                         ▼                   ▼
                    SUCCESSFUL         Retry / FAILED
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
* RestTemplate

## Infraestrutura

* Apache Kafka
* Docker
* Docker Compose

## Documentação e testes

* SpringDoc OpenAPI
* Swagger UI
* Postman Mock Server

---

# 📁 Estrutura do projeto

```text
src/main/java/com/juno/kafkaretryservice
│
├── config
│   └── RestTemplateConfig.java
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
  "reportPdfBase64": ""
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

O `ConcurrentHashMap` foi escolhido porque diferentes fluxos podem acessar as requisições simultaneamente.

```text
API REST
   ↓
cria e consulta Requests

Kafka Consumer
   ↓
busca Requests

ProcessingService
   ↓
atualiza Requests
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

O `KafkaTemplate` é utilizado para publicar mensagens no Kafka.

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

```text
Producer
→ publica

Consumer
→ recebe

ProcessingService
→ processa
```

---

# 🌐 Integração com API externa

O `ProcessingService` busca a Request pelo UUID recebido no evento.

Os dados da Request são transformados em um:

```text
RequestCreateDTO
```

Em seguida, o `RestTemplate` realiza um `POST` para a API externa.

```text
Request
   ↓
RequestCreateDTO
   ↓
RestTemplate
   ↓
HTTP POST
   ↓
API externa
```

No cenário de sucesso, a API externa retorna o mesmo DTO recebido.

```text
POST
↓
HTTP 200
↓
RequestCreateDTO
↓
SUCCESSFUL
```

A integração utilizada durante a demonstração é realizada com um Postman Mock Server.

---

# 🌍 RestTemplate

O `RestTemplate` é utilizado como cliente HTTP da aplicação.

Sua instância é disponibilizada pelo Spring através da classe:

```text
RestTemplateConfig
```

Exemplo:

```java
@Configuration
public class RestTemplateConfig {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
```

O Bean é injetado no `ProcessingService`.

```text
Spring
↓
RestTemplate Bean
↓
ProcessingService
```

Durante o processamento:

```text
RestTemplate
↓
POST
↓
API externa
↓
Resposta HTTP
```

Quando a resposta é bem-sucedida, o body JSON é convertido para:

```text
RequestCreateDTO
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

Erros HTTP da integração externa podem gerar exceções durante a chamada do `RestTemplate`.

```text
API externa
↓
4xx / 5xx
↓
Exceção
↓
@Retryable
```

---

# ✅ Cenário de sucesso

Quando a API externa responde corretamente:

```text
Tentativa 1
    ↓
POST na API externa
    ↓
HTTP 200
    ↓
DTO retornado
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

# ❌ Cenário de falha HTTP

Quando a integração externa retorna erro:

```text
Tentativa 1
    ↓
4xx / 5xx
    ↓
ERRO
    ↓
espera 10 segundos

Tentativa 2
    ↓
4xx / 5xx
    ↓
ERRO
    ↓
espera 10 segundos

Tentativa 3
    ↓
4xx / 5xx
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

Isso representa a falha definitiva da integração após todas as tentativas.

Sem o `@Recover`, a Request poderia permanecer em um estado intermediário mesmo após a falha do processamento.

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
    "reportPdfBase64": ""
  },
  {
    "accessionNumber": "10002",
    "studyDate": "2026-07-09T20:55:51",
    "studyDescription": "RX DE TORAX",
    "modality": "DX",
    "patientId": "P002",
    "patientName": "PACIENTE TESTE 02",
    "patientBirthDate": "1992-02-02",
    "patientSex": "F",
    "reportPdfBase64": ""
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
        ↓
é processado pelo Consumer
        ↓
é enviado para a API externa
```

---

# 📊 Estados da Request

## 🟡 `PENDING`

A requisição foi criada e aguarda processamento.

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

A API externa respondeu com sucesso e o processamento foi concluído.

```text
API externa
↓
HTTP 200
↓
DTO retornado
↓
SUCCESSFUL
```

---

## 🔴 `FAILED`

Todas as tentativas de processamento falharam.

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

Acompanhar os logs:

```bash
docker compose logs -f kafka
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

Com a aplicação em execução, acessar:

```text
localhost:8080/swagger-ui/index.html
```

No Swagger é possível:

* criar uma requisição;
* criar requisições em lote;
* listar requisições;
* consultar por UUID;
* acompanhar o status;
* acompanhar o número de tentativas.

---

# 🖥️ Logs da aplicação

Os logs foram mantidos simples para facilitar a demonstração.

Exemplo de sucesso:

```text
>> Requisição 2616003357 | Tentativa: 1
>> API externa respondeu para a requisição 2616003357
```

Resultado:

```text
SUCCESSFUL
```

Exemplo de falha HTTP:

```text
>> Requisição 2616003357 | Tentativa: 1

>> Requisição 2616003357 | Tentativa: 2

>> Requisição 2616003357 | Tentativa: 3

>> Requisição 2616003357 falhou definitivamente
```

Resultado:

```text
FAILED
```

---

# 🧩 Responsabilidades das classes

## `RestTemplateConfig`

Responsável por disponibilizar o `RestTemplate` como Bean gerenciado pelo Spring.

---

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

Responsável por publicar eventos no Kafka.

---

## `RequestConsumer`

Responsável por consumir eventos do Kafka e delegar o processamento.

---

## `ProcessingService`

Responsável pelo processamento e integração HTTP.

Contém:

```text
RestTemplate
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

14. Request é convertida em RequestCreateDTO

15. RestTemplate realiza POST na API externa

16A. HTTP 200:
     DTO é retornado
     SUCCESSFUL

16B. HTTP 4xx / 5xx:
     ocorre uma exceção
     @Retryable executa novamente

17. Tentativas esgotadas:
    @Recover

18. Status = FAILED
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
* configuração externa da URL da API;
* configuração externa do número de tentativas;
* configuração externa do backoff;
* testes automatizados;
* tratamento específico de exceções HTTP;
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

## Por que `RestTemplate`?

Para realizar a comunicação HTTP com a API externa.

```text
DTO
↓
RestTemplate
↓
HTTP POST
↓
API externa
↓
DTO de resposta
```

O cliente HTTP também permite que falhas da integração sejam propagadas para o mecanismo de retry.

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

Para tratar automaticamente falhas temporárias da integração.

Sem retry:

```text
Erro HTTP
↓
fim
```

Com retry:

```text
Erro HTTP
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
Integração HTTP
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
* ✅ Integração HTTP com API externa.
* ✅ `RestTemplate` configurado e funcionando.
* ✅ Envio do DTO para API externa.
* ✅ Retorno do DTO no cenário de sucesso.
* ✅ `@Retryable` funcionando.
* ✅ Backoff entre tentativas.
* ✅ `@Recover` funcionando.
* ✅ Cenário de sucesso.
* ✅ Fluxo preparado para erros HTTP.
* ✅ Processamento em lote.
* ✅ Swagger para demonstração.
* ✅ Postman Mock Server para integração externa.

---

# 👨‍💻 Autor

Desenvolvido por **Adeilson Maximiano Junior** como uma POC de processamento assíncrono e integração HTTP utilizando Apache Kafka, Spring Retry e RestTemplate.