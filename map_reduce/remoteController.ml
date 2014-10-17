open Async.Std

exception InfrastructureFailure
exception MapFailure of string
exception ReduceFailure of string

let worker_q = ref (AQueue.create ())
let starting_addrs = ref []
let valid_workers = ref []

let init addrs =
let store a = starting_addrs :=  a::(!starting_addrs) in
List.iter (store) addrs

(*removes an address from the list of valid addresses; if this list becomes
empty--which would imply that the remoteController has run out of workers, this
function will raise an InfrastructureFailure exception*)
let remove_bad_addr a =
let v_workers = !valid_workers in
valid_workers := List.filter ((<>) a) (v_workers);
match (!valid_workers) with
| [] -> raise InfrastructureFailure
| _ -> ()

module Make (Job : MapReduce.Job) = struct

  module Comb = Combiner.Make (Job)
  module WReq = Protocol.WorkerRequest (Job)
  module WResp = Protocol.WorkerResponse (Job)

(* Utility Functions **********************************************************)

  let start_connections () : unit Deferred.t =
    (*takes in a socket (ignored), reader, and writer and sends out the Job.name
      and then returns the reader and writer in a tuple*)
    let write_job (_,r,w) = Writer.write_line w (Job.name); (r,w) in
    let connect (h,p) =
      try_with (fun () -> Tcp.connect (Tcp.to_host_and_port h p) >>| write_job)
      >>| function
      | Core.Std.Result.Error _ -> () (*bad connection; don't add to queue*)
      | Core.Std.Result.Ok worker ->
          AQueue.push (!worker_q) worker;
          valid_workers := worker::(!valid_workers)
    in
    (return (!starting_addrs)) >>=
    (fun addrs -> Deferred.List.iter addrs connect)


  (*Executes a function on (s,r,w), a socket, reader, writer tuple using one
  of the workers accesible via the worker queue. Returns the result of f along
  with the reader, writer pair that executed it*)
  let execute f =
    let execute' (r,w) =
      try_with (fun () -> f (r,w)) >>| function
      | Core.Std.Result.Error e ->
          begin
            remove_bad_addr (r,w); (*remove the worker from queue, so that it*)
            (None, (r,w))          (*won't be used again*)
          end
      | Core.Std.Result.Ok res -> (Some res, (r,w))
    in
    AQueue.pop (!worker_q) >>= execute'

  (*looks at the read result from a pipe; if the pipe is closed, this function
    produces a failure, and otherwise, it returns the value in the pipe*)
  let check_for_closed_connection = function
    | `Eof -> failwith "connection was closed unexpetedly"
    | `Ok v -> v

(* Map Functions **************************************************************)

  (*looks at the contents of a map response, doing error handling if necessary
    and returning a result otherwise*)
  let rec process_map_response input (resp_opt, (r,w)) =
    match resp_opt with
    | None -> process_input input (*try processing the input again, but with
      a different worker (remember that execute will remove the bad worker
      from the queue )*)
    | Some (WResp.JobFailed str) -> raise (MapFailure str) (*note that this
      response isn't the result of a dropped connection, but an issue with
      the job--raise a failure since throwing a different worker at the
      problem won't resolve the issue*)
    | Some (WResp.ReduceResult _ ) ->
      (*if we get a ReduceResult from a MapRequest, something's off--
        remove the worker from the queue and try again*)
        begin
          remove_bad_addr (r,w);
          process_input input
        end
    | Some (WResp.MapResult res) ->
      (*success--add the worker back to queue, and return the result*)
        begin
          AQueue.push (!worker_q) (r,w);
          return res
        end

  and process_input input =
    let send_and_receive (r,w) =
      WReq.send w (WReq.MapRequest input);
      WResp.receive r >>| check_for_closed_connection
    in execute send_and_receive >>= process_map_response input 

(* Reduce Functions ***********************************************************)

  (*like process_map_response but for reduce responses*)
  let rec process_reduce_response inter (resp_opt, (r,w)) =
    match resp_opt with
    | None -> process_intermediate inter
    | Some (WResp.JobFailed str) -> raise (ReduceFailure str)
    | Some (WResp.MapResult _) ->
        remove_bad_addr (r,w);
        process_intermediate inter
    | Some (WResp.ReduceResult  res) ->
        AQueue.push (!worker_q) (r,w);
        return res

  and process_intermediate (k, ilist) =
    let send_and_receive (r,w) =
      WReq.send w (WReq.ReduceRequest (k,ilist));
      WResp.receive r >>| check_for_closed_connection
    in execute send_and_receive >>= process_reduce_response (k,ilist)

(* MapReduce Functions ********************************************************)

  let process_intermediate_output (k, ilist) =
    process_intermediate (k,ilist) >>= fun x -> return (k,x)

  let map_reduce inputs =
    if !valid_workers == [] then raise InfrastructureFailure
    else
      return inputs >>=
      Deferred.List.map ~how: `Parallel ~f: process_input >>|
      (fun lst -> Comb.combine (List.flatten lst)) >>=
      Deferred.List.map ~how: `Parallel ~f: process_intermediate_output >>|
      fun out_list -> worker_q := AQueue.create (); out_list

end

