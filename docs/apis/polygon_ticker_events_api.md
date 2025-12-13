# REST
## Stocks

### Ticker Events

**Endpoint:** `GET /vX/reference/tickers/{id}/events`

**Description:**

Retrieve a timeline of key events associated with a given ticker, CUSIP, or Composite FIGI. This endpoint is experimental and highlights ticker changes, such as symbol renaming or rebranding, helping users maintain continuity in their records and analyses.

Use Cases: Historical reference for ticker symbol changes, data continuity, and record-keeping.

## Path Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `id` | string | Yes | Identifier of an asset, which can be a Ticker, CUSIP, or Composite FIGI. Specify a case-sensitive  ticker symbol (e.g. AAPL for Apple Inc). When provided a ticker, events for the entity currently  represented by that ticker are returned. To find events for entities previously associated with a  ticker, obtain the relevant identifier using the [Ticker Details Endpoint](https://massive.com/docs/stocks/get_v3_reference_tickers__ticker). |

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `types` | string | No | A comma-separated list of the types of event to include. Currently ticker_change is the only supported event_type. Leave blank to return all supported event_types. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `request_id` | string | A request id assigned by the server. |
| `results` | object | Contains the requested event data for the specified ticker. |
| `results.events` | array[object] | An array of event containing the requested data. |
| `results.name` | string | The name of the asset. |
| `status` | string | The status of this request's response. |

## Sample Response

```json
{
  "request_id": "31d59dda-80e5-4721-8496-d0d32a654afe",
  "results": {
    "events": [
      {
        "date": "2022-06-09",
        "ticker_change": {
          "ticker": "META"
        },
        "type": "ticker_change"
      },
      {
        "date": "2012-05-18",
        "ticker_change": {
          "ticker": "FB"
        },
        "type": "ticker_change"
      }
    ],
    "name": "Meta Platforms, Inc. Class A Common Stock"
  },
  "status": "OK"
}
```


Ticker Name Change Event GWH
{
  "results": {
    "name": "ESS Tech, Inc.",
    "composite_figi": "BBG00Y4B6KS5",
    "cik": "0001819438",
    "events": [
      {
        "ticker_change": {
          "ticker": "GWH"
        },
        "type": "ticker_change",
        "date": "2024-08-26"
      }
    ]
  },
  "status": "OK",
  "request_id": "e19d72611a8548897ba86254516450d9"
}

GWH Stock Split
{
  "results": [
    {
      "execution_date": "2024-08-26",
      "id": "E62ffdc57f6ed2997d5cb7789c9b5787c0015e4386682a331ad58be805e17cc40",
      "split_from": 15,
      "split_to": 1,
      "ticker": "GWH"
    }
  ],
  "status": "OK",
  "request_id": "8facbaedaac7274d0dbfb12b8bffcc56"
}




