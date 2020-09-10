/**
 * mfm_kda_event.sql
 * by Steve Cooper
 */

CREATE OR REPLACE STREAM "DEST_FIX" (
  "MessageType" VARCHAR(32),
  "Symbol" VARCHAR(32),
  "Text" VARCHAR(32),
  "Quantity" int,
  "Price" decimal(18,2),
  "Notional" decimal(18,2),
  "Side" VARCHAR(32),
  "SideSign" int,
  "fixlog_session" VARCHAR(512),
  "fixlog_message"  VARCHAR(512),
  "fixlog_timestamp"  VARCHAR(32)
);

-- Create pump to insert into output 
CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "DEST_FIX"

-- Select all columns from source stream
SELECT STREAM
  CASE
    WHEN SUBSTRING("fixlog_message" FROM '\|35=([0-9A-Za-z])\|') = '|35=D|' THEN 'NewOrderSingle'
    WHEN SUBSTRING("fixlog_message" FROM '\|35=([0-9A-Za-z])\|') = '|35=8|' THEN 'Execution'
    WHEN SUBSTRING("fixlog_message" FROM '\|35=([0-9A-Za-z])\|') = '|35=j|' THEN 'BusinessReject'
    ELSE 'Other'
  END AS "MessageType",
  TRIM(TRAILING '|' FROM SUBSTRING(SUBSTRING("fixlog_message" FROM '\|55=([A-Z\.]+)\|') FROM 5)) as "Symbol",
  TRIM(TRAILING '|' FROM SUBSTRING(SUBSTRING("fixlog_message" FROM '\|58=([A-Za-z\.]+)\|') FROM 5)) as "Text",
  CAST(TRIM(TRAILING '|' FROM SUBSTRING(SUBSTRING("fixlog_message" FROM '\|38=([0-9]+)\|') FROM 5)) as int) as "Quantity",
--  CAST(TRIM(TRAILING '|' FROM SUBSTRING(SUBSTRING("fixlog_message" FROM '\|44=([0-9]+\.[0-9]+)\|') FROM 5)) as decimal) as "Price",
  CAST(TRIM(TRAILING '|' FROM SUBSTRING(SUBSTRING("fixlog_message" FROM '\|44=([0-9]*\.*[0-9]*)\|') FROM 5)) as decimal(18,2)) as "Price",
  CAST(TRIM(TRAILING '|' FROM SUBSTRING(SUBSTRING("fixlog_message" FROM '\|44=([0-9]*\.*[0-9]*)\|') FROM 5)) as decimal(18,2)) * CAST(TRIM(TRAILING '|' FROM SUBSTRING(SUBSTRING("fixlog_message" FROM '\|38=([0-9]+)\|') FROM 5)) as int) as "Notional",
  CASE
    WHEN SUBSTRING("fixlog_message" FROM '\|54=([0-9A-Z])\|') = '|54=1|' THEN 'Buy'
    WHEN SUBSTRING("fixlog_message" FROM '\|54=([0-9A-Z])\|') = '|54=2|' THEN 'Sell'
    ELSE 'Other'
  END AS "Side",
  CASE
    WHEN SUBSTRING("fixlog_message" FROM '\|54=([0-9A-Z])\|') = '|54=1|' THEN 1
    WHEN SUBSTRING("fixlog_message" FROM '\|54=([0-9A-Z])\|') = '|54=2|' THEN -1
    ELSE NULL
  END AS "SideSign",
  "fixlog_session" as "fixlog_session",
  "fixlog_message" as "fixlog_message",
  "fixlog_timestamp" AS "fixlog_timestamp"
FROM "SOURCE_SQL_STREAM_001"
;

CREATE OR REPLACE STREAM "DEST_GROSS_NOTIONAL_BY_SYMBOL" (
  "TotalGrossNotionalSymbol" VARCHAR(32),
  "TotalGrossQuantity" int,
  "TotalGrossNotional" decimal(18,2),
  "TotalGrossOrderCount" int
 , "EventTimeStamp" VARCHAR(32)
);
CREATE OR REPLACE PUMP "NOTIONAL_PUMP" AS
INSERT INTO "DEST_GROSS_NOTIONAL_BY_SYMBOL"
SELECT STREAM
  "TotalGrossNotionalSymbol",
  "TotalGrossQuantity",
  "TotalGrossNotional",
  "TotalGrossOrderCount"
  , TIMESTAMP_TO_CHAR('yyyy-MM-dd''T''HH:mm:ss.SSS', CURRENT_ROW_TIMESTAMP)
FROM (
  SELECT STREAM "Symbol" AS "TotalGrossNotionalSymbol",
  SUM("Quantity") OVER W1 AS "TotalGrossQuantity",
  SUM("Notional") OVER W1 AS "TotalGrossNotional",
  COUNT("Side") OVER W1 AS "TotalGrossOrderCount"
  FROM "DEST_FIX"
  WHERE "MessageType" = 'NewOrderSingle'
  WINDOW W1 AS (
    PARTITION BY "Symbol"
    RANGE INTERVAL '1' MINUTE PRECEDING))
WHERE "TotalGrossNotional" > 1000000000;
