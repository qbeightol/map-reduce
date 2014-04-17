
open Async.Std

let fork d f1 f2 = ignore (Deferred.both (d >>= f1) (d>>=f2))

let deferred_map l  (f: 'a -> 'b Deferred.t) =
    let f' e acc=
        acc
        >>= fun acc -> (f e)
        >>= fun mapped_e -> return (mapped_e::acc)
    in List.fold_right f' l (return []);;




