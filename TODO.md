1. Do not auto commit.
 
    1. Move ```consumerTopicOffsetService.recordConsumption()``` in the ConsumerJob 
    
    2. In case of exception in ```onMessage()```, do not record the consumption
    
2. Make global table with fields : 
    1. Consumer Group Id
    2. Topic Name
    3. Last consumed offset

    There will be UPDATE query to this table everytime a record is consumed.
    
    The __consumer_offsets table will not be used.