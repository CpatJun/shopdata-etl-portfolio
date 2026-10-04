-- Run against shopdata.db. Each query surfaces a different quality risk.
-- 1. Duplicate customer IDs and signup dates.
SELECT customer_id, COUNT(*) AS row_count, MIN(signup_date) AS first_signup, MAX(signup_date) AS latest_signup
FROM vw_raw_customers GROUP BY customer_id HAVING COUNT(*) > 1;

-- 2. Missing customer identifiers, signup dates, and contact details.
SELECT * FROM vw_raw_customers
WHERE customer_id IS NULL OR signup_date IS NULL OR TRIM(COALESCE(email, '')) = '' OR TRIM(COALESCE(phone, '')) = '';

-- 3. Non-positive or non-numeric order amounts (SQLite numeric cast is used as a signal).
SELECT * FROM vw_raw_orders
WHERE total_amount IS NULL OR CAST(total_amount AS REAL) <= 0;

-- 4. Missing order fields.
SELECT * FROM vw_raw_orders
WHERE order_id IS NULL OR customer_id IS NULL OR order_date IS NULL OR TRIM(COALESCE(currency, '')) = '';

-- 5. Orphan orders referencing no source customer.
SELECT o.* FROM vw_raw_orders o LEFT JOIN vw_raw_customers c ON c.customer_id=o.customer_id
WHERE c.customer_id IS NULL;

-- 6. Duplicate daily currency rates.
SELECT date, UPPER(TRIM(currency)) AS currency_key, COUNT(*) AS row_count
FROM vw_exchange_rates GROUP BY date, UPPER(TRIM(currency)) HAVING COUNT(*) > 1;

-- 7. Invalid or non-positive exchange rates.
SELECT * FROM vw_exchange_rates WHERE rate_to_usd IS NULL OR CAST(rate_to_usd AS REAL) <= 0;
