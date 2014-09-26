open Async.Std

module Make (Job : MapReduce.Job) = struct
  
  module WReq  = Protocol.WorkerRequest(Job)
  module WResp = Protocol.WorkerRequest(Job) 

  let rec process_requests () = unit Deferred.t = 
    WReq.receive r 
    >>= function 
    | `Eof -> return ()
    | `Ok (WReq.MapRequest j) -> 
        Job.map j 
        >>= fun result -> 
          (*once the worker has finished the job, send out the resulting list
            of (key, intermediate value) pairs and process the next request*)
          WResp.send w (WResp.mapResult result);
          process_requests ()
    | `Ok (WReq.ReduceRequest req) -> 
        Job.reduce req
        >>= fun out -> 
          WResp.send w (WResp.ReduceResult out);
          process_requests ()

  (* see .mli *)
  let run r w = process_requests ()
end

(* see .mli *)
let init port =
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port port)
    (fun _ r w ->
      Reader.read_line r >>= function
        | `Eof    -> return ()
        | `Ok job -> match MapReduce.get_job job with
          | None -> return ()
          | Some j ->
            let module Job = (val j) in
            let module Worker = Make(Job) in
            Worker.run r w
    )
    >>= fun _ ->
  print_endline "server started";
  print_endline "worker started.";
  print_endline "registered jobs:";
  List.iter print_endline (MapReduce.list_jobs ());
  never ()

