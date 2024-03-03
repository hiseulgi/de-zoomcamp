# Streaming Processing

## Introduction to Streaming Processing

Key takeaways:
- **Data exchange**: It's the process of sharing information between different sources. This can happen in various ways, like postal service, APIs, or even a public notice board.
- **Stream processing**: This is a specific type of data exchange where data is exchanged in real-time (with a slight delay, not at the speed of light). This is different from batch processing, where data is exchanged in larger chunks with longer delays (e.g., hourly or daily).

## Introduction to Apache Kafka

### What is Kafka?

[Apache Kafka](https://kafka.apache.org/) is a ***message broker*** and ***stream processor***. Kafka is used to handle ***real-time data feeds***.

Kafka is used to upgrade from a project architecture like this...

![no kafka](https://github.com/ziritrion/dataeng-zoomcamp/raw/main/notes/images/06_01.png)

...to an architecture like this:

![yes kafka](https://github.com/ziritrion/dataeng-zoomcamp/raw/main/notes/images/06_02.png)

In a data project we can differentiate between _consumers_ and _producers_:
* ***Consumers*** are those that consume the data: web pages, micro services, apps, etc.
* ***Producers*** are those who supply the data to consumers.

Connecting consumers to producers directly can lead to an amorphous and hard to maintain architecture in complex projects like the one in the first image. Kafka solves this issue by becoming an intermediary that all other components connect to.

Kafka works by allowing producers to send ***messages*** which are then pushed in real time by Kafka to consumers.

Kafka is hugely popular and most technology-related companies use it.

_You can also check out [this animated comic](https://www.gentlydownthe.stream/) to learn more about Kafka._

#### Kafka: Key Concepts

- **Kafka**: It's a streaming platform that acts as a central architecture for exchanging data in real-time.
- **Topics**: These are categories in Kafka that hold a continuous stream of events.
- **Events**: These are individual pieces of data within a topic, containing information like timestamps and specific data points.
  - **Messages**: Events are structured as messages in Kafka, containing a key, value, and timestamp.
    - **Key**: This is used to identify and partition the message within the topic.
    - **Value**: This is the actual data being exchanged in the message.
    - **Timestamp**: This indicates the exact time the message was created.
- **Benefits of Kafka**:
  - **Robustness**: Data is replicated across different servers to ensure reliability even if some nodes fail.
  - **Flexibility**: Topics can be scaled to various sizes and handle different numbers of consumers.
  - **Scalability**: Kafka can handle increasing data volumes efficiently.
- **Use cases**:
  - **Microservices communication**: Kafka facilitates communication between microservices by allowing them to exchange data through topics.
  - **Change Data Capture (CDC)**: Kafka can be used to capture changes from databases and send them as messages to topics, allowing other services to stay updated.