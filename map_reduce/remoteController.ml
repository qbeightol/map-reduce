open Async.Std

exception InfrastructureFailure
exception MapFailure of string 
exception ReduceFailure of string 

let addr_q = AQueue.create()
let valid_addrs = ref []

let init addrs = 
    let store a = valid_addrs :=  a::(!valid_addrs) in 
        List.iter (store) addrs

(*removes an address from the list of valid addresses; if this list becomes 
empty--which would imply that the remoteController has run out of workers, this
function will raise an InfrastructureFailure exception*)
let remove_bad_addr addr =
    valid_addrs := List.filter ((<>) addr) (!valid_addrs);
    match (!valid_addrs) with
    | [] -> raise InfrastructureFailure
    | _ -> ()
(*I might need to close the connection with the bad_addr too*)

let to_def_list lst =
    Deferred.List.init (List.length lst) (fun x -> return(List.nth lst x))

module Make (Job : MapReduce.Job) = struct
    (*Notice that this code is a functor called on Job, so it should be able to
    access the contents of Job. Notably, it'll be able to access
    
        Job.input
        Job.key
        Job.inter
        Job.output

        Job.name
        Job.map : input -> (key * inter) list Deferred.t
        Job.reduce : (key * inter list) -> output Deferred.t)

    Job.map and Job.reduce should be particularly useful.

    Also, I can pass along Job to the WorkerRequest and WorkerResponse functors
    *)
    module Comb = Combiner.Make (Job)
    module WReq = Protocol.WorkerRequest (Job)
    module WResp = Protocol.WorkerResponse (Job)

    (*Executes a function on (s,r,w), a socket, reader, writer tuple using one
    of the workers accesible via the address queue*)
    let execute f =
        AQueue.pop addr_q >>= 
        begin fun (h,p) -> 
            try_with (fun () -> Tcp.connect (Tcp.to_host_and_port h p) >>| f )
            >>| function
            | Core.Std.Result.Error _ -> remove_bad_addr (h,p)
            | Core.Std.Result.Ok _ -> AQueue.push addr_q (h,p)
        end

    let start_connections () : unit Deferred.t =
        let f (s,r,w) = Writer.write_line w Job.name in
        let connect (h,p) = 
            try_with (fun () -> Tcp.connect (Tcp.to_host_and_port h p) >>| f )
            >>| function
            | Core.Std.Result.Error _ -> remove_bad_addr (h,p)
            | Core.Std.Result.Ok _ -> AQueue.push addr_q (h,p)
        in 
        (to_def_list (!valid_addrs))
        >>= (fun addrs -> Deferred.List.iter addrs (connect))

            (*This seems nsafe; I should see if there's a better approach*)
            (*blah, at least it builds the addr_q*)

    let process_input (input : Job.input) (*: (Job.key * Job.inter) list Deferred.t *) =
        let f (s,r,w) = 
            WReq.send w (WReq.MapRequest input);
            WResp.receive r
            >>= fun resp -> 
                match resp with
                | `Eof -> failwith "connection was closed unexpectedly" 
                | `Ok resp ->
                    begin 
                        match resp with
                        | WResp.JobFailed s -> raise (MapFailure s)
                        | WResp.ReduceResult _ -> failwith "wrong response"
                        | WResp.MapResult l -> return l
                    end 
        in execute f
        (*fix so that it gets the requests from the workers*)

    let process_intermediate ((k : Job.key), (ilst: Job.inter list)) (*: Job.output Deferred.t *) =
        let f (s,r,w) = 
            WReq.send w (WReq.ReduceRequest (k, ilst));
            WResp.receive r
            >>= fun resp -> 
                match resp with
                | `Eof -> failwith "connection was closed unexpectedly" 
                | `Ok resp ->
                    begin 
                        match resp with
                        | WResp.JobFailed s -> raise (ReduceFailure s)
                        | WResp.MapResult _ -> failwith "wrong response"
                        | WResp.ReduceResult o -> return o
                    end 
        in execute f    

    (*val map_reduce : Job.input list -> (Job.key * Job.output) list Deferred.t*)
    let map_reduce inputs =
        failwith "you?"
        
        (*Deferred.List.map inputs (process_input) *)

        (*hmm, this will give something of type b' list Deferred.t
        I don't know that I want that to happen. But I don't know that it
        makes sense to do something like a fold or iter operation.
        Maybe I should do one operation that produces intermediate values and
        another operation that produces the final value. I'll use map for both
        and then stick a combine operation between the steps. *) 

        (*so something like this:*)
        start_connections ();
        Deferred.List.map inputs (process_input)
        >>| Comb.combine
        >>= (fun l -> Deferred.List.map l (process_intermediate))
        
        

        (*That may be all; I should check if I have to do anything before 
        returning the result of the reduce operation.

        Maybe I'll need to connect to workers before doing this...*)

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
    (*I think I'll use the command Tcp.connect or Tcp.with_connection so that
    I can connect to workers. I also will need to use Tcp.to_host_and_port.
    Specifically, I think I can call Tcp.to_host and port on the entries in the
    addresses passed to init, and then feed that result into Tcp.connect, and
    maybe Tcp.with_connection, too. I am a little confused about why we do
    this, though. At least, the documentation for TCP.connect seems to indicate
    that it accepts a host and a port, and not whatever Tcp.to_host_and_port
    returns. *)

    (*Apparently, I can pass the job info to workers using the Writer.write_line
    command. Hopefully, that's as simple as Writer.write_line Job. But that seems
    like a bad idea. Why not use Protocol.Marshaller.send w Job?*)

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

end

