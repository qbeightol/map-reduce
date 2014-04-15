open Async.Std

let worker_queue = AQueue.create()

let init addrs = 
    List.iter (AQueue.push worker_queue) addrs
    (*Apparently, this just stores the addresses for future use.
    Should I use aQueue here to remember what workers I can use,
    and which workers are avaiblable? *)

module Make (Job : MapReduce.Job) = struct
  let map_reduce inputs =

    (*note that you run map and reduce using Job.map and Job.Reduce (right?)*)
    (*Actually, I don't know that I'll have to use the above code. It probably
    will only get used by the worker. I do, however, need to send the data I've
    split-up/combined to the workers. That'll probably be done in the function
    I supply to the worker*)

    (*ERRORS*)
    (*If one worker dies, I'll need to ensure that I don't add it back to
    the queue, and I'll need to disregard its input. But I'm not totally
    sure how to detect for problems with input.*)
    (*There are a few cases to consider: I could be unable to connect to a
    worker, but I'm not sure how that problem manifests itself. It might
    happen early on during the connect to workers stage. 
    A connection could also break while it's in use. I'm not sure how to detect
    that, though. Maybe I'll get an `Eof from the socket.

    I also have to consider whether a worker
    might return an innapropriate response. But I'm not sure what that would
    look like, since everything's type checked. My best bet is that I might
    get an incomplete file (which seems like it would cause a type check
    error) or that a worker might send the wrong type of request (i.e. a
    MapResult instead of a ReduceResult). Actually, that seems satisfactory*)

    (*I also have to deal with jobfailures, but at least I can catch those when
    I'm matching the responses of workers*)

    (*CONNECT TO WORKERS*)
    (*How do I connect to workers, and how do I pass the job info to them?*)

    (*MAP*)
    (*For the map stage, I think I'll have to reach out to the workers and give
    them individual inputs to work on. So I'll probably need to split up inputs
    in some way. I'll also probably need to keep track of which workers are
    available and what has been done. *)
    (*To keep track of workers, I could use aQueue; At first I'd write every
    worker to the queue, and then I'd call pop for every input. I think I can
    do that all at once two--maybe I'd use our deferred_map operator or a
    large recursive function*)
    (*I'll also need to repopulate the queue with workers--I can probably do
    that by waiting for a worker to send a worker request (which should only be
    sent when the worker is free to process another input) *)
    (*Finally, I probably need a way to remember what's been processed. 
    Which might not be an issue if I use deferred map. *)
    
    (*COMBINE*)
    (*I think I just have to call combiner.combine on the result from the map
    phase. I might have to pull that list out a deferred type, but I'm not 
    sure*)
    
    (*REDUCE*)
    (*I think this is the same sort of problem as reduce--In fact I may be
    to create some function that works with both maps and reduces. *)
    
    (*RETURN*)
    failwith "Nowhere special."

end

