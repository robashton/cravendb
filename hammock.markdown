# Hammock time

- I need to stream a bunch of stuff
- I want to have push/start be the thing that waits for something to come from a channel and then shoves it out
- I want push/start to be able to take a function that can immediately return if there are already values available
- I want push/start to honour the media types that I can handle already

-  push/start should therefore take in a function that takes in a request and returns the resource available *now*
-  push/start should handle the return result of that much like we already do (standard-response)


- In the case of stats, that's just 'whatever is available right now'
- In the case of streaming, that's
    - Anything available from zero-synctag/synctag passed in
    - Maximum of 1000 at a time for example
    - If nothing is available, then we should hook the event from the database 
        - these are the events 'ifh' send out, ideally 'tx-committed'
        - Then we can invoke that first fn again and it'll return something by now




