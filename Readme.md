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

You can choose to either operate on single messages or send and receive messages in batch. If you use the batch mode, the server will expect multipart requests and return multipart responses instead of sending the message for example as a simple response body.

### Sending a message to a queue

```http
POST /messages/<queueName>
Content-Type: "application/json; charset=utf-8"

{
    "message": "data"
}
```

```http
POST /messages/<queueName>
Content-type: multipart/mixed; boundary="simple boundary"

--simple boundary
Content-Type: "application/json; charset=utf-8"

{
    "message": "data"
}
--simple boundary
Content-type: text/plain; charset=us-ascii

Another simple message.
--simple boundary--
```

### Receiving messages from a queue

```http
GET /messages/<queueName>
X-MQS-MAX-MESSAGES: <maxMessages, number, required>
X-MQS-WAIT-TIME: <waitTime, number, optional>
```

Function:

```haskell
if not exists queueName
    return 404
if maxMessages < 1 or maxMessages > 1000
    return 400
if set waitTime and (waitTime < 0 or waitTime > 60)
    return 400
messages = fetchMessages(...)
if messages.length > 0
    return 200, messages
if not set waitTime or waitTime = 0
    return 200, []
notifyWait queueLock up to waitTime
messages = fetchMessages(...)
    return 200, messages

catch error
    return 500
```

Responses:

- `200 OK` - Up to maxMessages were received. There might be less messages or no at all.
- `400 Bad Request` - One or multiple parameters did not validate. Body contains an error response.
- `404 Not Found` - The specified queue doesn't exist.
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

### Deleting messages from a queue

```http
DELETE /messages/<messageId>
```

Function:

```haskell
exists = deleteMessage(messageId)
if exists
    return 204
return 404

catch error
    return 500
```

Responses:

- `204 No Content` - The message was deleted and will no longer be returned.
- `404 Not Found` - The specified message does not exist  (you might want to treat this as success if you only wanted to assert the non-existence of a message).
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.
