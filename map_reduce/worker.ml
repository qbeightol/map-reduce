open Async.Std

module Make (Job : MapReduce.Job) = struct

  (* see .mli *)
  let run r w =
    print_endline "I'm doing something!";
    

    let module WReq = Protocol.WorkerRequest (Job) in
    let module WResp = Protocol.WorkerResponse (Job) in

    let rec process_requests () : unit Deferred.t =
      print_endline "waiting for message";
      WReq.receive r >>= function
      | `Eof -> print_endline "end of file"; return ()
      | `Ok m ->
        print_endline "received message"; 
        begin
          match m with 
          | WReq.MapRequest j -> 
            print_endline "got MapRequest";
            begin
              print_endline "processing MapRequest";
              (Job.map j) >>= function
              | [] -> print_endline "MapRequest contained an empty list"; return ()
              | kvp -> process_requests (WResp.send w (WResp.MapResult kvp))
            end
          | WReq.ReduceRequest (k, is) -> 
            print_endline "got ReduceRequest";
            begin
              print_endline "processing ReduceRequest";
              Job.reduce (k,is) >>= function
              | out -> process_requests (WResp.send w (WResp.ReduceResult out))
            end
          end
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
        | `Ok job -> print_endline ("JOB: "^job); match MapReduce.get_job job with
          | None -> print_endline "wah?"; return ()
          | Some j ->
            print_endline ("starting to run " ^job);
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

