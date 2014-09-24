open Async.Std

type 'a t = 'a Pipe.Reader.t * 'a Pipe.Writer.t
exception Closed_queue (*shouldn't be thrown--I haven't included any code that
                  closes pipes--but it's here in case a pipe does get closed*)

(* see .mli *)
let create () = Pipe.create()

(* see .mli *)
let push (_,w) x = 
  if Pipe.is_closed w then raise Closed_queue
  else Pipe.write_without_pushback w x 

(* see .mli *)
let pop  (r,_) = 
  Pipe.read r >>= function
                  | `Eof -> raise Closed_queue
                  | `Ok x -> return x




