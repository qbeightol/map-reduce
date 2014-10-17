open Async.Std

module Make (Job : MapReduce.Job) = struct
  
  module WReq  = Protocol.WorkerRequest(Job)
  module WResp = Protocol.WorkerResponse(Job)
  

  (* see .mli*)
  let run r w = 
    let rec process_requests () : unit Deferred.t = 
      WReq.receive r  >>= function 
      | `Eof -> return ()
      | `Ok (WReq.MapRequest input) -> 
          try_with (fun () -> Job.map input) >>= fun x -> 
          begin match x with
          | Core.Std.Ok res -> WResp.send w (WResp.MapResult res)
          | Core.Std.Error _ -> 
              WResp.send w (WResp.JobFailed (Job.name ^ ": Map Phase"))
          end;
          process_requests ()
      | `Ok (WReq.ReduceRequest (k, is)) -> 
          try_with (fun () -> Job.reduce (k, is)) >>= fun x -> 
          begin match x with 
          | Core.Std.Ok res -> WResp.send w (WResp.ReduceResult res);
          | Core.Std.Error _ -> 
              WResp.send w (WResp.JobFailed (Job.name ^ ": Reduce Phase"))
          end;
          process_requests ()
    in process_requests ()

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

