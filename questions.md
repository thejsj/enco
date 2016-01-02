1. When should I use a channel vs an exchange vs a queue? 
Messages should be published to exchanges which then route jobs to queues.
2. When the job is received, the queue already knows that a job has been sent to a particular worker (and only one worker is handling the job). The worker then needs to ACK or NACK the completion of that job, depending on whether that job was successful or not.
3. How does the publisher know when a job is finished? Should the publisher know when a job is finished?