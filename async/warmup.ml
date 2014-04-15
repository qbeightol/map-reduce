
open Async.Std

let fork d f1 f2 = ignore (Deferred.both (d >>= f1) (d>>=f2))

let deferred_map l  (f: 'a -> 'b Deferred.t) =
    let f' acc e =
        acc
        >>= fun acc -> (f e)
        >>= fun mapped_e -> return (mapped_e::acc)
    in List.fold_left f' (return []) l

(*
let rec d_m l (f: 'a ->'b Deferred.t) =
    match l with
    | [] -> []
    | hd::tl -> (f hd)::(d_m tl f)

let d_m2 l (f: 'a ->'b Deferred.t) =
    return (List.map f l) >>= (fun x -> return x)


(*The code below has the right type, but I don't think we can use it, and its
  really ugly. Also it may not work*)
let d_m3_hlp l =
    let f x = 
        match Deferred.peek x with
        | None -> failwith "uh oh"
        | Some x -> x
    in return (List.map f l)

let d_m3 l (f: 'a ->'b Deferred.t) =
    let tmp = List.map (fun x -> return x >>= f) l 
    in d_m3_hlp tmp

(*This looks more promising*)
let d_m4 l (f: 'a ->'b Deferred.t) =
    let f' acc e = acc >>= (fun acc -> (f e) >>= (fun x -> return (x::acc)))
    in List.fold_left f' (return []) l

let d_m5 l (f: 'a -> 'b Deferred.t) =
    let f' acc e =
        acc
        >>= fun acc -> (f e)
        >>= fun mapped_e -> return (mapped_e::acc)
    in List.fold_left f' (return []) l
*)

