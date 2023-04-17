{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

WITH payments AS (

  SELECT * 
  
  FROM {{ ref('stg_payments')}}

),

order_payments AS (

  SELECT 
    order_id,
    {% for payment_method in payment_methods %}
      sum(CASE
        WHEN payment_method = '{{ payment_method }}'
          THEN amount
        ELSE 0
      END) AS {{payment_method}}_amount,
    {% endfor %}
    
    sum(amount) AS total_amount
  
  FROM payments
  
  GROUP BY order_id

),

orders AS (

  SELECT * 
  
  FROM {{ ref('stg_orders')}}

),

final AS (

  SELECT 
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    orders.status,
    {% for payment_method in payment_methods %}
      order_payments.{{payment_method}}_amount,
    {% endfor %}
    
    order_payments.total_amount AS amount
  
  FROM orders
  LEFT JOIN order_payments
     ON orders.order_id = order_payments.order_id

),

Reformat AS (

  SELECT 
    order_id AS order_id,
    customer_id AS customer_id,
    order_date AS order_date,
    status AS status,
    credit_card_amount AS credit_card_amount,
    coupon_amount AS coupon_amount,
    bank_transfer_amount AS bank_transfer_amount,
    gift_card_amount AS gift_card_amount,
    amount AS amount,
    concat(order_id, status) AS ID_status
  
  FROM final AS in0

)

SELECT *

FROM Reformat
