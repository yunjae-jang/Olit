SELECT 
    p.productname,
    p.product_amt,
    AVG(diff) AS avg_diff,
    COUNT(*) AS total_orders,
    (SELECT 
        diff 
    FROM 
        (
            SELECT 
                p.productname, 
                DATEDIFF(
                    (
                        SELECT MIN(orderdate)
                        FROM orders_sl o2
                        WHERE o2.customer_key = o1.customer_key
                            AND o2.option_info LIKE CONCAT(p.productname, '★%')
                            AND o2.orderdate > o1.orderdate
                    ),
                    o1.orderdate
                ) AS diff, 
                COUNT(*) AS count 
            FROM 
                orders_sl o1
                JOIN products p ON replace(p.deal_option, ' / 1개', '') = replace(o1.option_info, '★', '')
            GROUP BY 
                customer_key, 
                productname 
            HAVING 
                COUNT(*) >= 2 
        ) t2
    WHERE 
        t2.productname = p.productname 
    GROUP BY 
        productname, 
        diff 
    ORDER BY 
        COUNT(*) DESC 
    LIMIT 1) AS mode_diff
FROM (
    SELECT
        replace(p.deal_option, ' / 1개', '') AS option_info,
        p.productname,
        p.product_amt,
        DATEDIFF(
            (
                SELECT MIN(orderdate)
                FROM orders_sl o2
                WHERE o2.customer_key = o1.customer_key
                    AND replace(o1.option_info, '★', '') LIKE CONCAT(replace(p.deal_option, ' / 1개', ''), '%')
                    AND o2.orderdate > o1.orderdate
            ),
            o1.orderdate
        ) AS diff
    FROM orders_sl o1
    JOIN products p ON replace(p.deal_option, ' / 1개', '') = replace(o1.option_info, '★', '')
    WHERE customer_key IN (
        SELECT customer_key
        FROM orders_sl
        GROUP BY customer_key
        HAVING COUNT(*) >= 2
    )
    GROUP BY
        replace(p.deal_option, ' / 1개', ''),
        p.productname,
        p.product_amt,
        diff
) o
JOIN products p ON replace(p.deal_option, ' / 1개', '') = o.option_info
GROUP BY 
    p.productname,
    p.product_amt;
use customer_imweb_ex;


SELECT 
    p.productname,
    p.product_amt,
    AVG(diff) AS avg_diff,
    COUNT(*) AS total_orders,
    (
        SELECT 
            diff 
        FROM 
            (
                SELECT 
                    p.productname, 
                    DATEDIFF(
                        (
                            SELECT MIN(orderdate)
                            FROM orders_sl o2
                            JOIN products p2 ON replace(p2.deal_option, ' / 1개', '') = replace(o2.option_info, '★', '')
                            WHERE o2.customer_key = o1.customer_key
                                AND p2.productname = p.productname
                                AND o2.orderdate > o1.orderdate
                        ),
                        o1.orderdate
                    ) AS diff, 
                    COUNT(*) AS count 
                FROM 
                    orders_sl o1
                    JOIN products p ON replace(p.deal_option, ' / 1개', '') = replace(o1.option_info, '★', '')
                WHERE customer_key IN (
                    SELECT customer_key
                    FROM orders_sl
                    GROUP BY customer_key
                    HAVING COUNT(*) >= 2
                )
                GROUP BY 
                    customer_key, 
                    p.productname, 
                    p.product_amt 
                HAVING 
                    COUNT(*) >= 2 
            ) t2
        WHERE 
            t2.productname = p.productname 
        GROUP BY 
            productname, 
            diff 
        ORDER BY 
            COUNT(*) DESC 
        LIMIT 1
    ) AS mode_diff
FROM (
    SELECT
        replace(p.deal_option, ' / 1개', '') AS option_info,
        p.productname,
        p.product_amt,
        DATEDIFF(
            (
                SELECT MIN(orderdate)
                FROM orders_sl o2
                JOIN products p2 ON replace(p2.deal_option, ' / 1개', '') = replace(o2.option_info, '★', '')
                WHERE o2.customer_key = o1.customer_key
                    AND p2.productname = p.productname
                    AND o2.orderdate > o1.orderdate
            ),
            o1.orderdate
        ) AS diff
    FROM orders_sl o1
    JOIN products p ON replace(p.deal_option, ' / 1개', '') = replace(o1.option_info, '★', '')
    WHERE customer_key IN (
        SELECT customer_key
        FROM orders_sl
        GROUP BY customer_key
        HAVING COUNT(*) >= 2
    )
    GROUP BY
        replace(p.deal_option, ' / 1개', ''),
        p.productname,
        p.product_amt,
        diff
) o
JOIN products p ON replace(p.deal_option, ' / 1개', '') = o.option_info
GROUP BY 
    p.productname,
    p.product_amt;

WITH max_product_amt AS (
    SELECT p.product_key, MAX(product_amt) AS max_product_amt
    FROM products p
    GROUP BY replace(p.deal_option, ' / 1개', '')
)
SELECT o.*, p.productname, p.product_amt
FROM orders o
JOIN products p ON replace(p.deal_option, ' / 1개', '') = replace(o.option_info, '★', '')
JOIN max_product_amt m ON p.product_key = m.product_key AND p.product_amt = m.max_product_amt
WHERE replace(o.option_info, '★', '') IN (
    SELECT DISTINCT replace(deal_option, ' / 1개', '') FROM products WHERE deal_option IS NOT NULL
)
AND o.option_info IS NOT NULL
AND DATE(o.orderdate) BETWEEN '2023-04-07' AND '2023-04-27'
AND o.option_info NOT LIKE '%100원%'
AND o.option_info NOT LIKE '%함께%'
and o.option_info like "%심플리커%"
AND (ordernum, product_price) IN (
    SELECT ordernum, MAX(product_price) FROM orders GROUP BY ordernum
);


SELECT
    p.productname,
    p.product_amt,
    AVG(diff) AS avg_diff,
    COUNT(*) AS total_orders,
    (
        SELECT
            diff
        FROM
            (
                SELECT
                    DATEDIFF(
                        (
                            SELECT MIN(orderdate)
                            FROM orders o2
                            JOIN products p2 ON replace(p2.deal_option, ' / 1개', '') = replace(o2.option_info, '★', '')
                            WHERE o2.customer_key = o1.customer_key
                                AND p2.productname = p.productname
                                AND o2.orderdate > o1.orderdate
                        ),
                        o1.orderdate
                    ) AS diff,
                    COUNT(*) AS count
                FROM
                    orders o1
                    JOIN products p ON replace(p.deal_option, ' / 1개', '') = replace(o1.option_info, '★', '')
                WHERE customer_key IN (
                    SELECT customer_key
                    FROM orders
                    GROUP BY customer_key
                    HAVING COUNT(*) >= 2
                )
                    AND p.productname = o.productname
                    AND p.product_amt = o.product_amt
                GROUP BY
                    diff
                ORDER BY
                    COUNT(*) DESC
                LIMIT 1
            ) t2
    ) AS mode_diff
FROM (
    SELECT
        replace(p.deal_option, ' / 1개', '') AS option_info,
        p.productname,
        p.product_amt,
        DATEDIFF(
            (
                SELECT MIN(orderdate)
                FROM orders o2
                JOIN products p2 ON replace(p2.deal_option, ' / 1개', '') = replace(o2.option_info, '★', '')
                WHERE o2.customer_key = o1.customer_key
                    AND p2.productname = p.productname
                    AND o2.orderdate > o1.orderdate
            ),
            o1.orderdate
        ) AS diff
    FROM orders o1
    JOIN products p ON replace(p.deal_option, ' / 1개', '') = replace(o1.option_info, '★', '')
    WHERE customer_key IN (
        SELECT customer_key
        FROM orders
        GROUP BY customer_key
        HAVING COUNT(*) >= 2
    )
    GROUP BY
        replace(p.deal_option, ' / 1개', ''),
        p.productname,
        p.product_amt,
        diff
) o
JOIN products p ON replace(p.deal_option, ' / 1개', '') = o.option_info
GROUP BY
    p.productname,
    p.product_amt;
    
select * from orders_cl;