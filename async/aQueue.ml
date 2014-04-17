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
        (*Hopefully, using write_without_pushback doesn't cause any problems; 
        I'm pretty certain that we don't care about the pushback, and I doubt 
        that will get in trouble,  since we only write when we know the pipe is
        open*)
        if not (Pipe.is_closed w) then Pipe.write_without_pushback w x 
        else raise Closed_queue

(* see .mli *)
let pop  (q: 'a t) =
    match q with (r, _) -> (Pipe.read r)
        >>= function
            | `Eof -> raise Closed_queue
            | `Ok x -> return x




