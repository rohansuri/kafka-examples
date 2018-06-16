Let's start by writing down various business transactions in e-commerce scenarios

(focus on backend, since we're doing kafka examples)


UI -> show up products
matching criteria given in search box
for now lets keep the criteria matching simple -- tags/category/product name/description

UI -> show one product (after we select it from the search result before)

UI -> place order for product (which includes purchasing it)
we select product, click buy/checkout, ask user for address, take them to payment gateway page, once confirmed, we confirm the order

payment gateway is external service? that gives callback to our service?
callback -- event-driven

what happens when confirming order?
* we have to reduce the product's stock quantity by 1 (or by the units purchased)

* we have to tell our shipping service, to start the shipping process

future scope:
there might be add-on services, that do re-pricing of product if let's say the same product is purchased a lot of times in a short interval, make it more expensive?

email service that mails summary when there are updates in order status/shipping status

email service that looks at product viewing history and mails users if the products are on discount?
