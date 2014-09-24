
open Async.Std

let fork d f1 f2 = ignore (d >>= f1); ignore (d>>= f2)

let my_both dx1 dx2 = dx1 >>= (fun x1 -> dx2 >>= fun x2 -> return (x1, x2))

let my_all (l: 'a Deferred.t list) : 'a list Deferred.t = 
  let f de dacc = my_both de dacc >>= (fun (e,acc) -> return (e::acc))
  in List.fold_right f l (return []) 

let deferred_map l f = my_all (List.map f l)





