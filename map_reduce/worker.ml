open Async.Std

module Make (Job : MapReduce.Job) = struct

  (* see .mli *)
  let run r w =
    WorkerRequest.receive r >>= function
    | `Eof -> return ()
    | `Ok m -> begin
      match m with
        MapRequest j ->
          Job.map j >>= function
          | [] -> return ()
          | kvp -> WorkerResponse.send w kvp
      | ReduceRequest kvp ->
        Job.reduce kvp >>= function
        | out -> WorkerResponse.send w out
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


