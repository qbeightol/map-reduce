open Async.Std

exception InfrastructureFailure
exception MapFailure of string 
exception ReduceFailure of string 

let worker_q = AQueue.create()
let starting_addrs = ref []
let valid_workers = ref []

let init addrs = 
    let store a = starting_addrs :=  a::(!starting_addrs) in 
        List.iter (store) addrs

(*removes an address from the list of valid addresses; if this list becomes 
empty--which would imply that the remoteController has run out of workers, this
function will raise an InfrastructureFailure exception*)
let remove_bad_addr (r,w) =
    valid_workers := List.filter ((<>) (r,w)) (!valid_workers);
    match (!valid_workers) with
    | [] -> raise InfrastructureFailure
    | _ -> ()

module Make (Job : MapReduce.Job) = struct

    module Comb = Combiner.Make (Job)
    module WReq = Protocol.WorkerRequest (Job)
    module WResp = Protocol.WorkerResponse (Job)

    let start_connections () : unit Deferred.t =
        let f (s,r,w) = Writer.write_line w (Job.name); (r,w) in
        let connect (h,p) = 
            try_with (fun () -> Tcp.connect (Tcp.to_host_and_port h p) >>| f )
            >>| function
            | Core.Std.Result.Error _ -> ()(*bad connection; don't add to queue*)
            | Core.Std.Result.Ok (r,w) -> 
                begin 
                    AQueue.push worker_q (r,w); 
                    valid_workers :=  (r,w)::(!valid_workers)
                end
        in 
        return (!starting_addrs)
        >>= (fun addrs -> Deferred.List.iter addrs (connect))

    (*Executes a function on (s,r,w), a socket, reader, writer tuple using one
    of the workers accesible via the worker queue. Returns the result of f along
    with the reader, writer pair that executed it*)
    let execute f =
        AQueue.pop worker_q >>=
        begin fun (r,w) ->
            try_with (fun () -> f (r,w))
            >>| function
            | Core.Std.Result.Error e -> remove_bad_addr (r,w); (None, (r,w))
            | Core.Std.Result.Ok res -> (Some res, (r,w))
        end

    let rec process_input input =
        let f (r,w) : WResp.t Deferred.t =
            WReq.send w (WReq.MapRequest input);
            WResp.receive r
            >>| begin fun resp ->
                begin
                    match resp with
                    | `Eof -> failwith "connection was closed unexpectedly" 
                    | `Ok resp -> resp
                end
            end 
        in execute f
        >>= begin fun (resp_opt, (r,w)) ->
            match resp_opt with
            | None -> process_input input
            | Some x ->
                begin
                    match x with
                    | WResp.JobFailed s -> raise (MapFailure s)
                    | WResp.ReduceResult _ -> 
                        begin
                            remove_bad_addr (r,w);
                            process_input input
                        end 
                    | WResp.MapResult l -> 
                        begin 
                            AQueue.push worker_q (r,w); 
                            return l
                        end
                end
            end

    let rec process_intermediate (k, ilst) =
        let f (r,w) : WResp.t Deferred.t =
            WReq.send w (WReq.ReduceRequest (k, ilst));
            WResp.receive r
            >>| begin fun resp ->
                begin
                    match resp with
                    | `Eof -> failwith "connection was closed unexpectedly" 
                    | `Ok resp -> resp 
                end
            end 
        in execute f
        >>= begin fun (resp_opt, (r,w)) ->
            match resp_opt with
            | None -> process_intermediate (k, ilst)
            | Some x ->
                begin
                    match x with
                    | WResp.JobFailed s -> raise (ReduceFailure s)
                    | WResp.MapResult _ -> 
                        begin
                            remove_bad_addr (r,w);
                            process_intermediate (k, ilst)
                        end 
                    | WResp.ReduceResult l -> 
                        begin
                            AQueue.push worker_q (r,w); 
                            return l
                        end
                end  
        end 

    (*val map_reduce : Job.input list -> (Job.key * Job.output) list Deferred.t*)
    let map_reduce inputs =
        (start_connections ())
        >>= (fun () ->
            if (!valid_workers) == [] then raise InfrastructureFailure
            else (return inputs)
            >>= (fun lst -> 
                Deferred.List.map ~how: `Parallel ~f: (process_input) lst)  
            >>| (fun lst -> Comb.combine (List.flatten lst))
            >>= (fun lst ->
                let f =  (fun (k, ilst) -> 
                    (process_intermediate (k, ilst)) 
                    >>= (fun x -> return (k, x)))
                in Deferred.List.map ~how: `Parallel ~f:f lst))

end

