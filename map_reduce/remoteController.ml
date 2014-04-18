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
        let f (s,r,w) = print_endline "writing job name"; Writer.write_line w (Job.name); print_endline (Job.name^"was written"); (r,w) in
        let connect (h,p) = 
            try_with (fun () -> Tcp.connect (Tcp.to_host_and_port h p) >>| f )
            >>| function
            | Core.Std.Result.Error _ -> print_endline "bad connection"
            | Core.Std.Result.Ok (r,w) -> print_endline ("good connection: "^h^" "^string_of_int p); AQueue.push worker_q (r,w); valid_workers :=  (r,w)::(!valid_workers)
        in 
        return (!starting_addrs)
        >>= (fun addrs -> Deferred.List.iter addrs (connect))

    (*Executes a function on (s,r,w), a socket, reader, writer tuple using one
    of the workers accesible via the worker queue. Returns the result of f along
    with the reader, writer pair that executed it*)
    let execute f =
        print_endline "queuing workers";
        AQueue.pop worker_q >>=
        begin fun (r,w) ->
            try_with (fun () -> f (r,w))
            >>| function
            | Core.Std.Result.Error e -> print_endline "error (not JobFailed) during execute"; remove_bad_addr (r,w); (None, (r,w))
            | Core.Std.Result.Ok res -> print_endline "work completeled succesfully; verifying results worker added back to the queue"; AQueue.push worker_q (r,w); (Some res, (r,w))
        end

    let rec process_input (input: Job.input) : (Job.key * Job.inter) list Deferred.t =
        let f (r,w) : WResp.t Deferred.t =
            print_endline "sending out a map request";
            WReq.send w (WReq.MapRequest input);
            print_endline "waiting for a map response";
            WResp.receive r
            >>| begin fun resp ->
                begin
                    match resp with
                    | `Eof -> failwith "connection was closed unexpectedly" 
                    | `Ok resp -> 
                        begin
                            print_endline "received a map response";
                            resp
                        end
                end
            end 
        in execute f
        >>= begin fun (resp_opt, (r,w)) ->
            print_endline "returning a map result";
            match resp_opt with
            | None -> print_endline "map failed; re-requesting"; process_input input
            | Some x ->
                begin
                    match x with
                    | WResp.JobFailed s -> raise (MapFailure s)
                    | WResp.ReduceResult _ -> 
                        begin
                            print_endline "worker provided the wrong response; removing it from the queue and re-requesting";
                            remove_bad_addr (r,w);
                            process_input input
                        end 
                    | WResp.MapResult l -> print_endline "valid map response"; return l
                end
            end

    let rec process_intermediate ((k : Job.key), (ilst: Job.inter list)) : Job.output Deferred.t  =
        let f (r,w) : WResp.t Deferred.t =
            print_endline "sending out a reduce request";
            WReq.send w (WReq.ReduceRequest (k, ilst));
            print_endline "waiting for a reduce response";
            WResp.receive r
            >>| begin fun resp ->
                begin
                    match resp with
                    | `Eof -> failwith "connection was closed unexpectedly" 
                    | `Ok resp -> 
                        begin
                            print_endline "received a reduce response";
                            resp
                        end
                end
            end 
        in execute f
        >>= begin fun (resp_opt, (r,w)) ->
            print_endline "returning a reduce result";
            match resp_opt with
            | None -> print_endline "reduce failed; re-requesting"; process_intermediate (k, ilst)
            | Some x ->
                begin
                    match x with
                    | WResp.JobFailed s -> raise (ReduceFailure s)
                    | WResp.MapResult _ -> 
                        begin
                            print_endline "worker provided the wrong response; removing it from the queue and re-requesting";
                            remove_bad_addr (r,w);
                            process_intermediate (k, ilst)
                        end 
                    | WResp.ReduceResult l -> print_endline "valid map response"; return l
                end  
        end 

    (*val map_reduce : Job.input list -> (Job.key * Job.output) list Deferred.t*)
    let map_reduce inputs =

        (start_connections ())
        >>= (fun () ->
            print_endline "connections started";
            if (!valid_workers) == [] then raise InfrastructureFailure
            else (return inputs)
            >>= (fun lst -> print_endline "mapping"; Deferred.List.map ~how: `Parallel ~f: (process_input) lst)  
            >>| (fun lst ->  print_endline "combining"; Comb.combine (List.flatten lst))
            >>= (fun lst -> print_endline "reducing"; Deferred.List.map ~how: `Parallel ~f: (fun (k, ilst) -> (process_intermediate (k, ilst)) >>= (fun x -> return (k, x))) lst))

end

