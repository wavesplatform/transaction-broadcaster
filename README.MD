## API
### GET /sequences/:id
#### Responses: ####

*200 OK*
```
{
    "id": <number>,
    "broadcastedCount": <number>,
    "totalCount": <number>,
    "hasError": <boolean>,
    "createdAt": <number>,
    "updatedAt": <number>
}
```

*404 Not Found*
```
{
    "message": "Sequence not found"
}
```

### POST /sequences
#### Request: ####
```
{
    "txs": [<array of strings>]
}
```

#### Responses: ####
*200 OK*
```
{
    "id": <number>  // sequence id
}
```
*400 Bad Request*
```
{
    "errors": [
        {
            "code": <number>,
            "message": <string>,
            "details: {}
        }
    ]
}
```

## Service environment variables
| # | Name | Type | Default | Description |
| - | ---- | ---- | ------- | ----------- |
| 1 | `PORT` | number | 3000 | Service port |
| 2 | `DEV` | boolean | false | Is dev mode |
| 3 | `NODE_URL` | string | - | Node URL that will be used to validate and broadcast txs |
| 4 | `NODE_API_KEY` | string | - | Node API Key, that will be used to validate txs |
| 5 | `PGHOST` | string | - | PostgreSQL host |
| 6 | `PGPORT` | number | 5432 | PostgreSQL port |
| 7 | `PGDATABASE` | string | - | PostgreSQL used database |
| 8 | `PGUSER` | string | - | PostgreSQL writer user login |
| 9 | `PGPASSWORD` | string | - | PostgreSQL writer user password |
| 10 | `DISPATCHER_DELAY` | number | 1000 | Number in ms - new sequences checking delay |
| 11 | `DISPATCHER_FROZEN_DURATION` | number | 600000 | Number in ms - in which duration sequences in state IsProcessing were not updated - after frozen duration these sequences state will be resetted, and dispatcher will take them out |
| 12 | `WORKERS_COUNT` | number | 20 | Max workers count |
| 13 | `WORKERS_SEQUENCE_PROCESSING_DELAY` | number | 1000 | Number in ms - worker pause after every sequence processing |
| 14 | `WORKERS_TX_PROCESSING_DELAY` | number | 1000 | Number in ms - worker pause after every tx processing |
