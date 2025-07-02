**SMQ** is a realtime distributed messaging platform designed to operate at scale, handling billions of messages per day, in low network latency and fast consumer, it can serve up to 300,000 messages per second

It promotes distributed and decentralized topologies without single points of failure, enabling fault tolerance and high availability coupled with a reliable message delivery guarantee. 

It's implemented from scratch with minimal 3rd party libraries.

Components:
----
**Broker**: core component, receives, queues, and delivers messages to clients.

It can be run standalone but is normally configured in a cluster with Monitor instance(s) (in which case it will announce topics and channels for discovery).

It listens on one TCP ports for serving clients.

**Monitor**: manages broker cluster information. Clients query monitor to discover brokers for broker nodes broadcasts topic and consumer group information.

There are two interfaces: A TCP interface which is used by brokers for broadcasts and an HTTP interface for clients to perform discovery actions.

**Clients** library for producer and consumer are also implemented in package cli