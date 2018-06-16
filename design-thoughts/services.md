We're doing things event driven
Hence not necessarily, all services wouldn't be REST based
i.e OrderService actually has nothing to offer as REST based
everything it does is through events

how would the Kafka based services look like in the spring world?

OrderService 
 - receives order, validates if sufficient quantity exists (remote query to start with, later using local cache streaming APIs)
 - after order validation, does the calling to external payment gateway, waits for it's callback (event)
 - puts events that triggers StockService reducing the available stock for ordered product

StockService
 - listen for stock updation events
 - put back stockUpdated event

CustomerService? (out of scope)
 - registers new customers
 - updates their details

 
 ________________________________________________________________________
 
 https://docs.spring.io/spring-kafka/reference/htmlsingle/
 https://www.confluent.io/blog/build-services-backbone-events/
 
 
 Maybe for starters just only focus on one business transaction?
 What Ben Stopford covered? 
 Which is Order came in, and we triggered notification for StockService?
 Also the local-view that OrderService will create to keep it's own view of the stocks?
 Rather than querying the StockService remotely sychronously? 
 