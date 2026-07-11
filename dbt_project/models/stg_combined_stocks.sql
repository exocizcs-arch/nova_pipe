WITH alpaca_data AS (
    SELECT
        CAST(timestamp AS DATE) AS trade_date,
        Ticker AS symbol,
        close AS close_price,
        volume,
        'alpaca' AS data_source
    FROM {{ source('landing_zone', 'alpaca_stocks') }}
),

yfinance_data AS (
    SELECT
        CAST("Date" AS DATE) AS trade_date,
        Ticker AS symbol,
        "Close" AS close_price,
        "Volume" AS volume,
        'yfinance' AS data_source
    FROM {{ source('landing_zone', 'yfinance_stocks') }}
)

SELECT * FROM alpaca_data
UNION ALL
SELECT * FROM yfinance_data