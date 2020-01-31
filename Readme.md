# Mini Queue Service (mqs)

[TOC]

## Operations on queues

mqs exposes an HTTP interface as its API.
Each operation is given with a rough specification of the request format, its function in pseudo-code, and a list of possible response codes and their meaning.
If an interval is given or expected, it is always expressed in seconds.
Changing attributes of queues might not affect messages already existing in the queue - for example, turning on message deduplication will not consider messages already in the queue when checking for duplicates.
On the other hand, a message does not remember its redrive policy, these settings are applied on message receive. 

### Create a new queue

```http
PUT /queues/<queue_name, string>
Content-Type: application/json

{
    "redrive_policy": {
        "max_receives": number,
        "dead_letter_queue": string
    } | null,
    "retention_timeout": number,
    "visibility_timeout": number,
    "message_delay": number,
    "message_deduplication": bool
}
```

Response:

```http
Content-Type: application/json

{
    "name": string,
    "redrive_policy": {
        "max_receives": number,
        "dead_letter_queue": string
    } | null,
    "retention_timeout": number,
    "visibility_timeout": number,
    "message_delay": number,
    "message_deduplication": bool
}
```

Function:

```haskell
if exists queue_name
    return 409
if does not parse (body)
    return 400
if set redrive_policy and not exists redrive_policy.dead_letter_queue
    return 400
if set redrive_policy.max_receives and (redrive_policy.max_receives < 1 or redrive_policy.max_receives > INT32_MAX)
    return 400
if retention_timeout < 1 or retention_timeout > INT32_MAX
    return 400
if visibility_timeout < 0 or visibility_timeout > INT32_MAX
    return 400
if set message_delay and (message_delay < 0 or message_delay > INT32_MAX)
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
POST /queues/<queue_name>
Content-Type: application/json

{
    "redrive_policy": {
        "max_receives": number,
        "dead_letter_queue": string
    } | null,
    "retention_timeout": number,
    "visibility_timeout": number,
    "message_delay": number,
    "message_deduplication": bool
}
```

Response:

```http
Content-Type: application/json

{
    "name": string,
    "redrive_policy": {
        "max_receives": number,
        "dead_letter_queue": string
    } | null,
    "retention_timeout": number,
    "visibility_timeout": number,
    "message_delay": number,
    "message_deduplication": bool
}
```

Function:

```haskell
if not exists queue_name
    return 404
if does not parse (body)
    return 400
if set redrive_policy and not exists redrive_policy.dead_letter_queue
    return 400
if set redrive_policy.max_receives and (redrive_policy.max_receives < 1 or redrive_policy.max_receives > INT32_MAX)
    return 400
if retention_timeout < 1 or retention_timeout > INT32_MAX
    return 400
if visibility_timeout < 0 or visibility_timeout > INT32_MAX
    return 400
if set message_delay and (message_delay < 0 or message_delay > INT32_MAX)
    return 400
updateQueue(...)
return 200

catch error
    return 500
```

Responses:

- `200 Ok` - The queue was successfully updated and can now be used. Messages will not become visible if you reduce the visibility timeout and a message is already hidden, will not be moved to dead letter queues or deleted until you receive the message again.
- `400 Bad Request` - One or multiple parameters did not validate. Body contains an error response.
- `404 Not Found` - A queue with the same name does not exists.
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

### Delete an existing queue

```http
DELETE /queues/<queue_name, string>
```

Response:

```http
Content-Type: application/json

{
    "name": string,
    "redrive_policy": {
        "max_receives": number,
        "dead_letter_queue": string
    } | null,
    "retention_timeout": number,
    "visibility_timeout": number,
    "message_delay": number,
    "message_deduplication": bool
}
```

Function:

```haskell
if not exists queue_name
    return 404
if any queue has queue_name as deadLetterQueue
    update queue set deadLetterQueue = None, maxReceives = None
deleteQueueAndMessages(...)
return 200

catch error
    return 500
```

Responses:

- `200 OK` - The queue was successfully deleted and can no longer be used.
- `404 Not Found` - The specified queue doesn't exist (you might want to treat this as success if you only wanted to assert the non-existence of a queue).
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

### Listing all queues

```http
GET /queues?offset=<offset, number, optional>&limit=<limit, number, optional>
```

Response:

```http
Content-Type: application/json

{
    "total": number,
    "queues": [
        {
            "name": string,
            "redrive_policy": {
                "max_receives": number,
                "dead_letter_queue": string
            } | null,
            "retention_timeout": number,
            "visibility_timeout": number,
            "message_delay": number,
            "message_deduplication": bool
        }
    ]
}
```

Responses:

- `200 OK` - Valid result with queue data.
- `400 Bad Request` - You did not specify numbers for `offset` or `limit`.
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

### Getting attributes of one queue

```http
GET /queues/<queue_name>
```

Response:

```http
Content-Type: application/json

{
    "name": string,
    "redrive_policy": {
        "max_receives": number,
        "dead_letter_queue": string
    } | null,
    "retention_timeout": number,
    "visibility_timeout": number,
    "message_delay": number,
    "message_deduplication": bool,
    "status": {
        "messages": number,
        "visible_messages": number,
        "oldest_message_age": number
    }
}
```

Responses:

- `200 OK` - Valid result with queue data.
- `404 Not Found` - The queue with the given name does not exist.
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

## Operations on messages

For now we only support handling a single message at a time.
While batch operations might provide higher throughput, we value simplicity over performance for now. If you need high performance, a managed service like SQS or something more complex and scalable like RabbitMQ might be an alternative for you.

### Sending a message to a queue

Note: To avoid loading too large messages into memory, the size of a message is limited to 1 MB.
If you send more data, it is ignored and cut off after the limit.
The format of a message is not fixed to json.
Instead, you can send any content type and content and will receive the same content again when receiving a message.
So you can without problems send XML, MessagePack or any other (binary) data to a queue and will receive the same data later again.
If you do not specify a content type, it defaults to `application/octet-stream`.

```http
POST /messages/<queue_name>
Content-Type: "application/json"

{
    "any_json_data": "data"
}
```

Function:

```haskell
if not exists queue_name
    return 404
exists = publishMessage(contentType, body.take(1MB))
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
GET /messages/<queue_name>
```

Function:

```haskell
if not exists queue_name
    return 404
message = fetchMessage(...)
if messages is some
    return 200, message
return 204

catch error
    return 500
```

Responses:

- `200 OK` - A message was found and is returned in the response. The header `X-MQS-MESSAGE-ID` contains the id of the message. `Content-Type` is set to the content type set during message receive.
- `204 No Content` - No message was found, try again after some time or publishing a message.
- `404 Not Found` - The specified queue doesn't exist.
- `500 Internal Server Error` - The server hit an unexpected error condition and can not continue.

### Deleting messages from a queue

```http
DELETE /messages/<message_id>
```

Function:

```haskell
if not isUuid(message_id)
    return 400
exists = deleteMessage(message_id)
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
