--Build staging CTE for cleaning and changing data types in gold_data

DROP TABLE IF EXISTS stg_gold;

CREATE VIEW stg_gold AS
SELECT
    substr(Date,7,4) || '-' ||
    substr(Date,4,2) || '-' ||
    substr(Date,1,2) AS date,
    CAST(Close AS REAL) AS close,
    CAST(Open AS REAL) AS open,
    CAST(High AS REAL) AS high,
    CAST(Low AS REAL) AS low,
    CAST(Volume AS INTEGER) AS volume
FROM gold_data;

DROP TABLE IF EXISTS gold_processed;
CREATE VIEW stg_gold_processed AS
SELECT
    substr(DATE,7,4) || '-' ||
    substr(DATE,4,2) || '-' ||
    substr(DATE,1,2) AS date,
    CAST(close AS REAL) AS close,
    CAST(ret AS REAL) AS ret 
FROM gold_processed;

DROP TABLE IF EXISTS stg_forecast;

CREATE VIEW stg_forecast AS
SELECT
    substr(ds,7,4) || '-' ||
    substr(ds,4,2) || '-' ||
    substr(ds,1,2) AS date,
    CAST(yhat AS REAL) AS yhat,
    CAST(yhat_lower AS REAL) AS yhat_lower,
    CAST(yhat_upper AS REAL) AS yhat_upper
FROM forecast_prophet;

DROP TABLE IF EXISTS stg_features;

CREATE VIEW stg_features AS
SELECT
    substr(Date,7,4) || '-' ||
    substr(Date,4,2) || '-' ||
    substr(Date,1,2) AS date,
    CAST(close AS REAL) AS close,
    substr(ds,7,4) || '-' ||
    substr(ds,4,2) || '-' ||
    substr(ds,1,2) AS forecast_date,
    CAST(y AS REAL) AS y,
    CAST(lag_1 AS REAL) AS lag_1,
    CAST(lag_2 AS REAL) AS lag_2,
    CAST(lag_3 AS REAL) AS lag_3,
    CAST(lag_5 AS REAL) AS lag_5,
    CAST(lag_10 AS REAL) AS lag_10,
    CAST(lag_21 AS REAL) AS lag_21,
    CAST(rolling_mean_7 AS REAL) AS rolling_mean_7,
    CAST(rolling_mean_21 AS REAL) AS rolling_mean_21,
    CAST(rolling_mean_63 AS REAL) AS rolling_mean_63
FROM feature;

--Fact view for return value and forecast accuracy view
DROP TABLE IF EXISTS fact_gold;

CREATE VIEW fact_gold AS
WITH base AS (
SELECT
    date,
    close,
    high,
    low,
    open,
    volume,
    LAG(close) OVER (ORDER BY date) AS prev_close
FROM stg_gold
)
SELECT
    date,
    close,
    high,
    low,
    open,
    volume,
    CASE
        WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
        ELSE (close/prev_close) - 1
    END AS ret
FROM base;

DROP TABLE IF EXISTS gold_vs_forecast;

CREATE VIEW gold_vs_forecast AS
SELECT
    g.date,
    g.close AS actual_close,
    f.yhat AS forecast_close,
    f.yhat_lower,
    f.yhat_upper,
    (g.close - f.yhat) AS error,
    CASE
        WHEN g.close IS NULL OR f.yhat IS NULL THEN NULL
        ELSE ABS(g.close - f.yhat) / g.close
    END AS ape
FROM fact_gold g 
LEFT JOIN stg_forecast f 
ON f.date = g.date;

--QA check
--Check duplicate dates
SELECT date, COUNT(*) AS cnt 
FROM stg_gold
GROUP BY date 
HAVING cnt > 1;

--Check missing close values
SELECT COUNT(*) AS null_close_rows
FROM stg_gold
WHERE close IS NULL;

--Sanity check
SELECT *
FROM stg_gold
WHERE close < low OR close > high;

--Compare raw-return vs processed-return
SELECT
    r.date,
    r.ret AS ret_from_raw,
    p.ret AS ret_processed,
    ABS(r.ret - p.ret) AS diff
FROM fact_gold r 
JOIN stg_gold_processed p 
    ON r.date = p.date 
WHERE r.ret IS NOT NULL 
    AND p.ret IS NOT NULL
    AND ABS(r.ret - p.ret) > 0.0001
ORDER BY diff DESC
LIMIT 20;

--business analysis: overall trend of gold price by years
SELECT
    strftime('%Y' , date) AS year,
    ROUND(AVG(close), 2) AS avg_close,
    ROUND(MAX(close), 2) AS max_close,
    ROUND(MIN(close), 2) AS min_close
FROM fact_gold
GROUP BY year
ORDER BY year;

--trend by month,volatility with percentage return
SELECT
    strftime('%m' , date) AS month,
    ROUND(AVG(ret)*100, 3) AS avg_ret,
    ROUND(sqrt(AVG(ret*ret)-AVG(ret)*AVG(ret))*100, 3) AS vol_pct
FROM fact_gold
WHERE ret IS NOT NULL
GROUP BY month
ORDER BY month;

--Best and worst day of return
SELECT
    date,
    ROUND(ret*100, 3) AS ret_pct
FROM fact_gold
WHERE ret IS NOT NULL
ORDER BY ret_pct DESC
LIMIT 10;

--a volitality proxy: average of absolute return by month(how much prices move regardless of direction)
SELECT
    strftime('%Y-%m' , date) AS month,
    ROUND(AVG(ABS(ret))*100, 3) AS avg_abs_ret
FROM fact_gold
WHERE ret IS NOT NULL
GROUP BY month
ORDER BY month;

--Max peak by date,how much did the price fall from that peak by date,the worst dradown  
WITH peak AS (
SELECT
    date,
    close,
    MAX(close) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS peak
FROM fact_gold
),
dd AS (
SELECT
    date,
    close,
    peak,
    (close - peak) / peak AS drawdown
FROM peak
)
SELECT
    date,
    close,
    peak,
    ROUND(drawdown*100, 2) AS drawdown_pct
FROM dd 
ORDER BY drawdown ASC
LIMIT 10;

--gold go up,down or stay flat
SELECT
    CASE
        WHEN ret > 0 THEN 'UP'
        WHEN ret < 0 THEN 'DOWN'
        ELSE 'FLAT/NULL'
    END AS movement,
    COUNT(*) AS days
FROM fact_gold
GROUP BY movement;

--How good is my forecast compared to actual values?
SELECT
    ROUND(AVG(error), 2) AS MAE,
    ROUND(AVG(ape)*100, 2) AS MAPE_pct
FROM gold_vs_forecast
WHERE ape IS NOT NULL;

--How accurate is my forecast when volatility market is moving bigger?
WITH joined AS (
SELECT
    g.date,
    ABS(g.ret) AS abs_ret,
    v.ape
FROM fact_gold g
JOIN gold_vs_forecast v
ON v.date = g.date
WHERE g.ret IS NOT NULL AND v.ape IS NOT NULL
)
SELECT
    CASE
        WHEN abs_ret >= 0.02 THEN 'High Volatility (>=2%)'
        ELSE 'Normal Volatility (<2%)'
    END AS regime,
    COUNT(*) AS days,
    ROUND(AVG(ape)*100, 2) AS MAPE_pct
FROM joined
GROUP BY regime;

--How accurate is my forecast too high or too low?(over or under prediction)
--error > 0 → forecast was too low
--error < 0 → forecast was too high
SELECT
    ROUND(AVG(error), 2) AS mean_error,
    ROUND(AVG(CASE WHEN error > 0 THEN 1 ELSE 0 END)*100, 2) AS pct_over_forecast
    FROM gold_vs_forecast
WHERE error IS NOT NULL;

--Out of all trading days, what percentages of days did the price go up compared to the previous day?
WITH movement AS (
SELECT
    date,
    close,
    lag_1,
    (close - lag_1) AS delta_close
FROM stg_features
WHERE lag_1 IS NOT NULL
)
SELECT
    ROUND(AVG(CASE WHEN delta_close > 0 THEN 1 ELSE 0 END)*100, 2) AS pct_up_days
FROM movement;

--Compare today's price with the recent 21-day average price and measure the difference
SELECT
    date,
    close,
    rolling_mean_21,
    ROUND(close - rolling_mean_21, 2) AS above_below_rm21
FROM stg_features
WHERE rolling_mean_21 IS NOT NULL
ORDER BY date DESC
LIMIT 30;

--performance index
CREATE INDEX IF NOT EXISTS idx_gold_price ON gold_data(Date);
CREATE INDEX IF NOT EXISTS idx_gold_processed_date ON gold_processed(DATE);
CREATE INDEX IF NOT EXISTS idx_forecast_ds ON forecast_prophet(ds);
CREATE INDEX IF NOT EXISTS idx_features_date ON feature(Date);
