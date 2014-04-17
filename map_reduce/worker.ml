open Async.Std

module Make (Job : MapReduce.Job) = struct

  (* see .mli *)
  let run r w =
  (*At some point, we'll need to use mapReduce.get_job on the job name sent to
  the worker. Then we can make modules out of the WorkerRequest and 
  WorkerResponse functors.*)

  (*Get the job name from the remote controller*)
  Reader.read_line r 
  >>= function
  | `Eof -> return ()
  | `Ok name -> 
    match (MapReduce.get_job name) with
    | None -> return ()
    | Some (module J : MapReduce.Job) ->
      begin
        (*Do i need ins here?*)
        
        let module WReq = Protocol.WorkerRequest (J) in
        let module WResp = Protocol.WorkerResponse (J) in

        WReq.receive r >>= function
        | `Eof -> return ()
        | `Ok m -> 
          begin
            match m with 
            | WReq.MapRequest j -> 
              begin
                J.map j >>= function
                | [] -> return ()
                | kvp -> return (WResp.send w (WResp.MapResult kvp))
              end
            | WReq.ReduceRequest (k, is) -> 
              begin
                J.reduce (k,is) >>= function
                | out -> return (WResp.send w (WResp.ReduceResult out))
              end
          end
      end 
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


