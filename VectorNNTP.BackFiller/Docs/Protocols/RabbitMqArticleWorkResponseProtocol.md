# BackFiller RabbitMQ Article-Work Response Protocol

## Canonical Status
This document defines the canonical BackFiller RPC response protocol for article-work processing over RabbitMQ.

- Protocol name: BackFiller RabbitMQ Article-Work Response
- Version: 1
- Scope: BackFiller consumer -> RabbitMQ RPC response queue

## Transport Contract (AMQP)
Responses use standard RabbitMQ RPC semantics:

- Publish destination: incoming request `ReplyTo` property
- Response AMQP `CorrelationId`: incoming request `CorrelationId` property
- ContentType: `application/json`

`CorrelationId`, `ReplyTo`, and AMQP response `MessageId` are transport metadata and are never duplicated in response JSON payloads.

AMQP response `MessageId` is generated as a fresh UUID for each response publication attempt (each call to `PublishAndConfirmAsync`, including retries/republications). It is distinct from JSON `messageId`, JSON `requestId`, and AMQP `CorrelationId`.

JSON Schema: `RabbitMqArticleWorkResponse.v1.schema.json`.

## Response JSON Protocol (v1)
Response JSON includes exactly these fields:

- `version`
- `requestId`
- `messageId`
- `backbone`
- `outcome`
- `uri` (success only)
- `error` (terminal failures only)

Identity availability contract:

- `requestId`, `messageId`, and `backbone` represent request-body identity values when those values were successfully parsed before failure.
- BackFiller never fabricates synthetic request identity for failure responses.
- For `InvalidRequest` only, any of these identity fields may be `null` when parsing failed before that identity could be established.
- For `Success`, `ArticleNotFound`, and `InvalidArticle`, identity fields remain concrete, non-empty request-body values.

Canonical success response payload:

```json
{"version":1,"requestId":"7c1cb8a0-95f9-4c13-8e53-339773e3afaa","messageId":"<12345@example.invalid>","backbone":"Giganews","outcome":"Success","uri":"cache://backfiller01.usenet.ninja:119/30edc94157aa16fe644a45a1f1ffe160"}
```

Canonical terminal failure response payload:

```json
{"version":1,"requestId":"7c1cb8a0-95f9-4c13-8e53-339773e3afaa","messageId":"<12345@example.invalid>","backbone":"Giganews","outcome":"ArticleNotFound","error":"No article with that message-id"}
```

## Response Property Contract
| Property | Type | Required | Description |
|---|---|---:|---|
| `version` | Integer | Yes | Application-level response schema version. Current value: `1`. |
| `requestId` | String (GUID) or `null` | Yes | Original application request identity from request JSON body when available; `null` is permitted only for `InvalidRequest` when request identity could not be parsed. |
| `messageId` | String or `null` | Yes | Original Message-ID from request JSON body when available; `null` is permitted only for `InvalidRequest` when message identity could not be parsed. |
| `backbone` | String or `null` | Yes | Original backbone from request JSON body when available; `null` is permitted only for `InvalidRequest` when backbone identity could not be parsed. |
| `outcome` | String | Yes | Terminal outcome classification string. |
| `uri` | String | No | Success-only canonical article cache URI: `cache://{CanonicalBackFillerFqdn}:{BindPort}/{MessageIdMd5}` where `MessageIdMd5` is lowercase hexadecimal MD5 of ASCII bytes of the exact `messageId` string. |
| `error` | String | No | Required and non-empty for response-carrying terminal failure outcomes; omitted for success. |

## Outcome and Disposition Matrix
| Processing Outcome | RPC Response | RabbitMQ Disposition | Requeue |
|---|---|---|---:|
| Success | Yes (required): `outcome=Success`, `uri` present and formatted as `cache://{CanonicalBackFillerFqdn}:{BindPort}/{MessageIdMd5}` | ACK | N/A |
| ArticleNotFound | Yes (required terminal failure): `outcome=ArticleNotFound`, `error` present | NACK | false |
| InvalidArticle | Yes (required terminal failure): `outcome=InvalidArticle`, `error` present | NACK | false |
| InvalidRequest | Replyable only: `outcome=InvalidRequest`, `error` required/non-empty, identity fields preserve parsed values and may be `null` when unavailable | NACK | false |
| ProviderFailure | No terminal response | NACK | true |
| Cancelled | No terminal response | NACK | true |
| UnexpectedFailure | No terminal response | NACK | true |

## Success Ordering Invariant
For `Success`, BackFiller enforces:

1. Process article
2. Build response JSON
3. Publish to `ReplyTo` with AMQP `CorrelationId`
4. Wait for publisher confirm (bounded by `PublishConfirmTimeoutSeconds`)
5. ACK original request `DeliveryTag`

BackFiller does not ACK before response publish confirm.

## Confirm Failure/Timeout Behavior
If response publish fails or confirm times out:

- original request is not ACKed
- request is NACKed with `requeue=true`
- message remains retryable

## Canonical Success URI Contract
For `outcome=Success`, `uri` is normative and MUST use this grammar:

`cache://{CanonicalBackFillerFqdn}:{BindPort}/{MessageIdMd5}`

Where:
- `CanonicalBackFillerFqdn`: exact authoritative `BackFillerRuntimeOptions.CanonicalBackFillerFqdn` value.
- `BindPort`: exact authoritative `BackFillerRuntimeOptions.BindPort` value.
- `MessageIdMd5`: lowercase hexadecimal MD5 digest (exactly 32 hexadecimal characters) of the ASCII bytes of the exact `messageId` string from the request payload.

Hashing input and transform rules:
- Input is the exact `messageId` string already accepted by BackFiller request validation.
- Do not lowercase `messageId`.
- Do not trim or strip whitespace.
- Do not remove angle brackets (`<` and `>`).
- Do not apply any additional normalization before hashing.

Deterministic vectors used by repository tests:
- `<12345@example.invalid>` -> `30edc94157aa16fe644a45a1f1ffe160`
- `<abc@example.invalid>` -> `de438dc83d64b1fa9206cf4da9eed5cc`

## Identity and Redelivery Semantics
- `messageId`: canonical article identity from JSON request body and the identity used to derive `uri` when outcome is `Success`.
- `requestId`: application work-request identity from JSON request body; not used as article URI identity.
- `CorrelationId`: AMQP RPC identity from transport properties.
- `MessageId` (AMQP response property): transport-level response publication identity, generated as a new UUID for each publication attempt.
- `DeliveryTag`: AMQP delivery-settlement identity.
- `ConnectionGeneration`: BackFiller infrastructure identity.

Invalid-request identity rules:
- BackFiller never synthesizes fake request identity values to satisfy protocol shape.
- For `InvalidRequest`, identity fields preserve any successfully parsed request-body identity and set only unavailable identity fields to `null`.
- BackFiller does not substitute delivery backbone/correlation metadata into request-body identity fields.

Response publication for invalid requests:
- If AMQP `CorrelationId` or `ReplyTo` is missing, the invalid request remains non-replyable and no response is published.
- If both are present, an `InvalidRequest` response is published with required non-empty `error`.

Redelivery preserves request-body identity values and `CorrelationId`; `DeliveryTag`, `ConsumerIdentity`, and `ConnectionGeneration` may change.

## Limitations
This phase does not implement deduplication or exactly-once response semantics. A crash after response publish confirm but before ACK may lead to duplicate responses on redelivery.
