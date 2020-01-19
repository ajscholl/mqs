# Mini Queue Service (mqs)

[TOC]

## Operations on queues

mqs exposes an hTTP interface as its API. Each operation is given with a rough specification of the request format, its function in pseudo-code, and a list of possible response codes and their meaning.

### Create a new queue

```http
PUT /queues/<queueName, string>
X-MQS-MAX-RECEIVES: <maxReceives, number, optional>
X-MQS-DEAD-LETTER-QUEUE: <deadLetterQueueName, string, optional>
X-MQS-RETENTION-SECONDS: <retentionSeconds, number, required>
X-MQS-VISIBILITY-TIMEOUT-SECONDS: <visibilityTimeoutSeconds, number, required>
X-MQS-DELAY-SECONDS: <delaySeconds, number, optional>
X-MQS-CONTENT-BASED-DEDUPLICATION: <contentBasedDeduplication, bool, required>
```

Function:

```haskell
if exists queueName
    return 409
if set deadLetterQueueName and not exists deadLetterQueueName
    return 400
if set maxReceives and (maxReceives < 1 or maxReceives > INT32_MAX)
    return 400
if set deadLetterQueue != set maxReceives
    return 400
if retentionSeconds < 1 or retentionSeconds > INT32_MAX
    return 400
if visibilityTimeout < 0 or visibilityTimeout > INT32_MAX
    return 400
if set delaySeconds and (delaySeconds < 0 or delaySeconds > INT32_MAX)
    return 400
createQueue(...)
return 201

catch error
    return 500
```

Responses:

- `201 Created` - The queue was successfully created and can now be used.
- `400 Bad Request` - One or multiple parameters did not validate. Body contains an error response.
- `409 Conflict` - A queue with the same name already exists.
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

### Update properties of a queue

```http
POST /queues/<queueName>
X-MQS-MAX-RECEIVES: <maxReceives, number, optional>
X-MQS-DEAD-LETTER-QUEUE: <deadLetterQueueName, string, optional>
X-MQS-RETENTION-SECONDS: <retentionSeconds, number, required>
X-MQS-VISIBILITY-TIMEOUT-SECONDS: <visibilityTimeoutSeconds, number, required>
X-MQS-DELAY-SECONDS: <delaySeconds, number, optional>
```

Function:

```haskell
if not exists queueName
    return 404
if set deadLetterQueueName and not exists deadLetterQueueName
    return 400
if set maxReceives and (maxReceives < 1 or maxReceives > INT32_MAX)
    return 400
if set deadLetterQueue != set maxReceives
    return 400
if retentionSeconds < 1 or retentionSeconds > INT32_MAX
    return 400
if visibilityTimeout < 0 or visibilityTimeout > INT32_MAX
    return 400
if set delaySeconds and (delaySeconds < 0 or delaySeconds > INT32_MAX)
    return 400
updateQueue(...)
return 200

catch error
    return 500
```

Responses:

- `200 Created` - The queue was successfully updated and can now be used. Messages will not become visible if you reduce the visibility timeout and a message is already hidden, will not be moved to dead letter queues or deleted until you receive the message again.
- `400 Bad Request` - One or multiple parameters did not validate. Body contains an error response.
- `404 Not Found` - A queue with the same name does not exists.
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

### Delete an existing queue

```http
DELETE /queues/<queueName, string>
X-MQS-CASCADE: <cascade, true | false, default true>
```

Function:

```haskell
if not exists queueName
    return 404
if any queue has queueName as deadLetterQueue
    if not cascade
        return 412
    update queue set deadLetterQueue = None, maxReceives = None
deleteQueue(...)
deleteMessages(...)
return 200

catch error
    return 500
```

Responses:

- `200 OK` - The queue was successfully deleted and can no longer be used.
- `400 Bad Request` - One or multiple parameters did not validate. Body contains an error response.
- `404 Not Found` - The specified queue doesn't exist (you might want to treat this as success if you only wanted to assert the non-existence of a queue).
- `412 Precondition Failed` - A queue uses the given queue as a dead letter queue.
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

### Listing all queues

```http
GET /queues
Range: users=20-40 (optional)
```

Response:

```http
Content-Type: application/json; encoding=utf-8
Accept-Ranges: queues
Content-Range: queues 0-9/200

{
    "queues": [
        {
            "name": "queueName",
            "deadLetterQueue": "deadLetterQueueName" or null,
            "maxReceives": maxReceives or null,
            "retentionSeconds": retentionSeconds,
            "visibilityTimeoutSeconds": visibilityTimeoutSeconds,
            "delaySeconds": delaySeconds or 0
        }
    ]
}
```

Responses:

- `200 OK` - Valid result with queue data.
- `416 Request Range Not Satisfyable` - You did not specify a valid range.
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

### Getting attributes of one queue

```http
GET /queues/<queueName>
```

Response:

```http
Content-Type: application/json

{
    "name": "queueName",
    "deadLetterQueue": "deadLetterQueueName" or null,
    "maxReceives": maxReceives or null,
    "retentionSeconds": retentionSeconds,
    "visibilityTimeoutSeconds": visibilityTimeoutSeconds,
    "delaySeconds": delaySeconds or 0
}
```

Responses:

- `200 OK` - Valid result with queue data.
- `404 Not Found` - The queue with the given name does not exist.
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

## Operations on messages

For now we only support handling a single message at a time. While batch operations might provide higher throughput, we value simplicity over performance for now. If you need high performance, a managed service like SQS or something more complex and scalable like RabbitMQ might be an alternative for you.
### Sending a message to a queue

```http
POST /messages/<queueName>
Content-Type: "application/json; charset=utf-8"

{
    "message": "data"
}
```

Function:

```haskell
if not exists queueName
    return 404
exists = publishMessage(...)
if exists
    return 200
else
    return 201

catch error
    return 500
```

Responses:

- `200 OK` - The message did already exist in the queue, no action has been performed.
- `201 Created` - The message was successfully published to the queue.
- `404 Not Found` - The specified queue doesn't exist.
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

### Receiving messages from a queue

```http
GET /messages/<queueName>
```

Function:

```haskell
if not exists queueName
    return 404
message = fetchMessage(...)
if messages is some
    return 200, message
return 204

catch error
    return 500
```

Responses:

- `200 OK` - A message was found and is returned in the response. The header `X-MQS-MESSAGE-ID` contains the id of the message.
- `204 No Content` - No message was found, try again after some time or publishing a message.
- `404 Not Found` - The specified queue doesn't exist.
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

### Deleting messages from a queue

```http
DELETE /messages/<messageId>
```

Function:

```haskell
if not isUuid(messageId)
    return 400
exists = deleteMessage(messageId)
if exists
    return 204
return 404

catch error
    return 500
```

Responses:

- `204 No Content` - The message was deleted and will no longer be returned.
- `400 Bad Request` - The specified message id is not a valid uuid.
- `404 Not Found` - The specified message does not exist  (you might want to treat this as success if you only wanted to assert the non-existence of a message).
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.
