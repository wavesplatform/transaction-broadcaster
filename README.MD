## API
### GET /sequences/:id
#### Responses: ####

*200 OK*
```
{
    "id": <number>,
    "broadcastedCount": <number>,
    "totalCount": <number>,
    "state" :<string>,   // one of sequence states
    "errorMessage": <string>,
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
    "transactions": [<array of object transaction>]
}
```

#### Responses: ####
*201 Created*
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
            "details: <Record<string,any>>
        }
    ]
}
```

## Sequence states

1. `pending` - sequence is pending processing
2. `processing` - sequence is being processed
3. `done` - after last tx there is `HEIGHTS_AFTER_LAST_TX` blocks in the blockchain
4. `error` - check the `errorMessage` sequence field


## Service environment variables
| # | Name | Type | Default | Description |
| - | ---- | ---- | ------- | ----------- |
| 1 | `PORT` | number | 3000 | Service port |
| 2 | `DEV` | boolean | false | Is dev mode |
| 3 | `PGHOST` | string | - | PostgreSQL host |
| 4 | `PGPORT` | number | 5432 | PostgreSQL port |
| 5 | `PGDATABASE` | string | - | PostgreSQL used database |
| 6 | `PGUSER` | string | - | PostgreSQL writer user login |
| 7 | `PGPASSWORD` | string | - | PostgreSQL writer user password |
| 8 | `WAVES_NODE_URL` | string | - | Node URL that will be used to validate and broadcast txs |
| 9 | `WAVES_NODE_API_KEY` | string | - | Node API Key, that will be used to validate txs |
| 10 | `WAVES_WAIT_FOR_TX_STATUS_DELAY` | number | 1000 | Number in ms - delay to recheck tx status |
| 11 | `WAVES_WAIT_FOR_TX_TIMEOUT` | number | 90000 | Number in ms - time after which tx status checking is considering as failed (by default ~1.5 block) |
| 12 | `WAVES_WAIT_FOR_NEXT_HEIGHT_DELAY` | number | 1000 | Number in ms - time after which worker will check the blockchain height again |
| 13 | `DISPATCHER_LOOP_DELAY` | number | 1000 | Number in ms - check for hanging sequences delay |
| 14 | `DISPATCHER_SEQUENCE_TTL` | number | 5000 | Number in ms - in which time sequences in state `processing` were not updated and have to be reseted, and dispatcher will take them out |
| 15 | `WORKER_TX_PROCESSING_TTL` | number | 3000 | Number in ms - after which time transactions in state `processing` were not updated and have to be retaken |
| 16 | `WORKER_HEIGHTS_AFTER_LAST_TX` | number | 6 | Number - after which blocks number sequence is considered as done |
| 17 | `WORKER_WAIT_FOR_NEXT_HEIGHT_DELAY` | number | 1000 | Number in ms - time after which worker will check the blockchain height again |
| 18 | `WORKER_NUMBER_OF_REVALIDATE_ATTEMPTS` | number | 6 | Number - number of transaction revalidate attemts |
