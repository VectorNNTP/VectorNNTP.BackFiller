# BackFiller RabbitMQ Article-Work Request Protocol

## Canonical Status
This document defines the canonical BackFiller article-work request format for RabbitMQ.

- Protocol name: BackFiller RabbitMQ Article-Work Request
- Version: 1
- Scope: Producer -> RabbitMQ -> BackFiller consumer processing pipeline
- Compatibility note: This protocol is intentionally new and is not a compatibility shim for undocumented legacy Grabber payload formats.

## Purpose
Provide a compact, explicit, versioned JSON application payload for high-volume article retrieval work while preserving standard RabbitMQ RPC metadata in AMQP properties.

## Layered Contract (Authoritative)
An incoming work item consists of three explicit layers:

1. RabbitMQ AMQP RPC properties (transport metadata)
   - `CorrelationId`
   - `ReplyTo`
2. JSON application payload body
   - `version`
   - `requestId`
   - `messageId`
   - `backbone`
3. RabbitMQ/BackFiller delivery metadata
   - `DeliveryTag`, `Redelivered`, `RoutingKey`, `Exchange`, `ConsumerTag`
   - `ConnectionGeneration`, `ConsumerIdentity`

`CorrelationId` and `ReplyTo` are deliberately NOT part of the JSON body.

## Canonical JSON Application Payload
```json
{"version":1,"requestId":"7c1cb8a0-95f9-4c13-8e53-339773e3afaa","messageId":"<12345@example.invalid>","backbone":"Giganews"}
```

## Canonical Incoming Request Example
RabbitMQ properties:
- `CorrelationId`: `61d4b5f6-8b24-4c14-8d40-741a378abfc8`
- `ReplyTo`: `nnrpd.rpc.responses`
- `ContentType`: `application/json`

JSON body:
```json
{"version":1,"requestId":"7c1cb8a0-95f9-4c13-8e53-339773e3afaa","messageId":"12345@example.invalid","backbone":"Giganews"}
```

The JSON does NOT contain `CorrelationId`.
The JSON does NOT contain `ReplyTo`.

## JSON Property Contract
| Property | Type | Required | Null Allowed | Description |
|---|---|---:|---:|---|
| `version` | Integer | Yes | No | Application-level schema version. Current value: `1`. |
| `requestId` | String (GUID) | Yes | No | Application work-request identifier. Stable across redelivery. Distinct from delivery metadata and AMQP correlation. |
| `messageId` | String | Yes | No | Usenet Message-ID to retrieve (`ARTICLE <message-id>`). Preserved as supplied except existing NNTP validation requirements. |
| `backbone` | String | Yes | No | Target backbone identity. Must match consuming queue/session backbone context. |

## Required/Optional Fields
Version 1 defines only required JSON fields. No optional JSON fields are defined.

## JSON Serialization/Deserialization Behavior
- JSON library: `System.Text.Json`.
- Property names are case-sensitive and must match exactly: `version`, `requestId`, `messageId`, `backbone`.
- Parser validates type and value constraints for all four required JSON properties.
- Unknown JSON fields are ignored for forward compatibility.
- Property ordering is not significant.
- Whitespace is ignored.
- Canonical producer serialization is compact JSON with no indentation.

## AMQP RPC Property Requirements
BackFiller RPC contract requires both AMQP properties to be present:
- `CorrelationId`: required; treated as opaque transport correlation value.
- `ReplyTo`: required; treated as opaque transport reply destination.

BackFiller does not manufacture missing values.
Missing `CorrelationId` or `ReplyTo` is classified as `InvalidRequest`.

## Validation Rules
A request is rejected as `InvalidRequest` when any condition is true:
- null/empty payload bytes
- invalid JSON
- JSON root is not an object
- missing `version`
- unsupported `version`
- missing or invalid `requestId` GUID
- missing or invalid `messageId`
- missing or invalid `backbone`
- `backbone` does not match consuming queue/session backbone context
- missing AMQP `CorrelationId`
- missing AMQP `ReplyTo`

Malformed protocol payloads/metadata are classified as `InvalidRequest`, never `ProviderFailure`.

## Backbone Semantics
`backbone` is explicit in JSON and is an integrity boundary:
- Consumer already has queue-scoped backbone context.
- JSON `backbone` must match that context.
- Comparison follows existing BackFiller conventions: case-insensitive ordinal comparison.
- Mismatch prevents provider retrieval against the wrong backbone.

## Identity Semantics
- `requestId` (JSON): application request identity, stable across redelivery.
- `CorrelationId` (AMQP): RPC transaction identity, transport-level source of truth.
- `DeliveryTag` (AMQP): delivery identity for future ACK/NACK operations.
- `ConnectionGeneration` (BackFiller runtime): infrastructure lifecycle identity.

These identities are distinct and must not be merged.

## Redelivery Semantics
RabbitMQ may redeliver the same message.
Expected to remain stable across redelivery:
- JSON `requestId`
- AMQP `CorrelationId`
- AMQP `ReplyTo`
- JSON `messageId`
- JSON `backbone`

May change across redelivery or consumer recovery:
- `DeliveryTag`
- `ConsumerIdentity`
- `ConnectionGeneration`

ACK/NACK policy is defined by the response/disposition protocol and remains transport metadata driven.

## Versioning Strategy
- `version` is the application-level compatibility key.
- Version `1` is the current canonical schema.
- Unsupported versions must be rejected as `InvalidRequest`.
- Future schema changes must increment `version` and preserve explicit parser behavior per version.

## Invalid Message Behavior
Invalid messages are:
- parsed into failure results with outcome `InvalidRequest`
- never upgraded to provider failures
- not logged with complete payload dumps

## Security Considerations
- JSON payload MUST NOT contain credentials, tokens, private keys, or article bodies.
- Parser and processing logs must not emit full request payloads.
- Diagnostics should report validation reason only.
- RabbitMQ/NNTP credentials are runtime configuration concerns and not protocol fields.

## Example Valid JSON Payload
```json
{"version":1,"requestId":"d0648b54-b1b8-4717-95e1-7b31bf7fd1bd","messageId":"<abc@example.invalid>","backbone":"Eweka"}
```

## Example Invalid JSON Payload
```json
{"version":2,"requestId":"not-a-guid","messageId":"","backbone":"WrongBackbone"}
```
Reasons: unsupported version, invalid requestId, empty messageId, and backbone mismatch against queue context.

## Future Extensibility Guidance
- Do not add speculative fields in v1.
- Add fields only with explicit versioning and parser updates.
- Keep payload minimal for high-volume transport.
- Preserve strict separation between JSON application payload and AMQP transport metadata.
