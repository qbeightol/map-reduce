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
            | Core.Std.Result.Error _ -> remove_bad_addr (h,p); None
            | Core.Std.Result.Ok res -> AQueue.push addr_q (h,p); Some (res)
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

            (*This seems unsafe; I should see if there's a better approach*)
            (*blah, at least it builds the addr_q*)

    let rec process_input (input : Job.input) : (Job.key * Job.inter) list Deferred.t  =
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
                        | WResp.MapResult l -> (return l)
                    end 
        in execute f 
        >>= (fun res ->
            match res with
            | None -> process_input input
            | Some x -> x)
        (*fix so that it gets the requests from the workers*)

    let rec process_intermediate ((k : Job.key), (ilst: Job.inter list)) : Job.output Deferred.t  =
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
        >>= (fun res ->
            match res with
            | None -> process_intermediate (k, ilst)
            | Some x -> x)    

    (*val map_reduce : Job.input list -> (Job.key * Job.output) list Deferred.t*)
    let map_reduce inputs =
        start_connections ()
        >>= (fun () -> Warmup.deferred_map inputs (process_input))
        >>| (fun lst -> Comb.combine (List.flatten lst))
        >>= (fun lst -> Warmup.deferred_map lst (fun (k, ilst) -> (process_intermediate (k, ilst)) >>= (fun x -> return (k, x)) ))

end

