## KafkaManager Pre-requisites

- [ ] Ensure that any ACL that is created, is created for a resource that already exists

- [ ] Ensure RITM is approved

- [ ]  PVSR signed off CM task approved

- [ ] If a new topic is created, deleted, or updated, make sure that you have that topic reflected in the topics_config_{env}.csv and the topics_{env}.json


### Topic Resource Management

#### Add topic

- [ ] Ensure that partition count is no more than 32 and no less than 3

- [ ] Ensure that retention.ms is no more than 7 days or x ms.

- [ ] Ensure that max.message.bytes is no more than 5 Mebibytes

#### Update Topic

- [ ] Ensure that partition count is no more than 32 and no less than 3

- [ ] Ensure that retention.ms is no more than 7 days or x ms.

- [ ] Ensure that max.message.bytes is no more than 5 Mebibytes

#### Delete Topic

- [ ] Go to C3 and verify that storage on the topic is 0 bytes

- [ ] Go to C3 and Ensure that there is no consumer lag

### ACL Resource Management



### Connector Resource Management