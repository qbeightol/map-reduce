open Async.Std

type 'a t = 'a Pipe.Reader.t * 'a Pipe.Writer.t
exception Closed_queue (*shouldn't be thrown--I haven't included any code that
                  closes pipes--but it's here in case a pipe does get closed*)

(* see .mli *)
let create () =
    Pipe.create()

(* see .mli *)
let push (q: 'a t) x =
    match q with (_, w) -> 
        if not (Pipe.is_closed w) then Pipe.write_without_pushback w x 
        else raise Closed_queue

(* see .mli *)
let pop  (q: 'a t) =
    match q with (r, _) -> (Pipe.read r)
        >>= function
            | `Eof -> raise Closed_queue
            | `Ok x -> return x




