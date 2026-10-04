
-- Historical customer value from completed orders.
-- Keep all customers, including those with no completed orders.

SELECT
    c.customer_id,
    c.full_name,

    COUNT(
        CASE
            WHEN o.status = 'COMPLETED'
            THEN o.order_id
        END
    ) AS completed_orders,

    ROUND(
        COALESCE(
            SUM(
                CASE
                    WHEN o.status = 'COMPLETED'
                    THEN o.usd_amount
                    ELSE 0
                END
            ),
            0
        ),
        2
    ) AS historical_lifetime_value_usd,

    strftime('%Y-%m', c.signup_date) AS customer_cohort

FROM dim_customers AS c

LEFT JOIN fct_orders AS o
    ON o.customer_id = c.customer_id

GROUP BY
    c.customer_id,
    c.full_name,
    c.signup_date

ORDER BY
    historical_lifetime_value_usd DESC,
    c.customer_id;
