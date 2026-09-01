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

`CorrelationId` and `ReplyTo` are transport metadata and are never duplicated in response JSON payloads.

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

Canonical success response payload:

```json
{"version":1,"requestId":"7c1cb8a0-95f9-4c13-8e53-339773e3afaa","messageId":"<12345@example.invalid>","backbone":"Giganews","outcome":"Success","uri":null}
```

Canonical terminal failure response payload:

```json
{"version":1,"requestId":"7c1cb8a0-95f9-4c13-8e53-339773e3afaa","messageId":"<12345@example.invalid>","backbone":"Giganews","outcome":"ArticleNotFound","error":"No article with that message-id"}
```

## Response Property Contract
| Property | Type | Required | Description |
|---|---|---:|---|
| `version` | Integer | Yes | Application-level response schema version. Current value: `1`. |
| `requestId` | String (GUID) | Yes | Original application request identity from request JSON body. |
| `messageId` | String | Yes | Original Message-ID from request JSON body. |
| `backbone` | String | Yes | Original backbone from request JSON body. |
| `outcome` | String | Yes | Terminal outcome classification string. |
| `uri` | String | No | Optional retrieval location. Omitted when no stable URI contract exists. |
| `error` | String | No | Optional terminal error detail for response-carrying non-success outcomes. |

## Outcome and Disposition Matrix
| Processing Outcome | RPC Response | RabbitMQ Disposition | Requeue |
|---|---|---|---:|
| Success | Yes (required): `outcome=Success`, `uri` present (`null` when no stable location exists) | ACK | N/A |
| ArticleNotFound | Yes (required terminal failure): `outcome=ArticleNotFound`, `error` present | NACK | false |
| InvalidArticle | Yes (required terminal failure): `outcome=InvalidArticle`, `error` present | NACK | false |
| InvalidRequest | Yes (required terminal failure): `outcome=InvalidRequest`, `error` present | NACK | false |
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

## Identity and Redelivery Semantics
- `requestId`: application identity from JSON request body
- `CorrelationId`: AMQP RPC identity from transport properties
- `DeliveryTag`: AMQP delivery-settlement identity
- `ConnectionGeneration`: BackFiller infrastructure identity

Redelivery preserves `requestId` and `CorrelationId`; `DeliveryTag`, `ConsumerIdentity`, and `ConnectionGeneration` may change.

## Limitations
This phase does not implement deduplication or exactly-once response semantics. A crash after response publish confirm but before ACK may lead to duplicate responses on redelivery.
