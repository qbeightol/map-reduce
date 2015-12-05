
open Async.Std

(*takes in [dx] (read deferred x) that waits until dx has been determined and 
  then supplies the resulting value, x, to f1 and f2, ignoring f1's and f2's
  output *)
let fork dx f1 f2 = ignore (dx >>= f1); ignore (dx >>= f2)

(*note: to implement deferred_map, I could only use the functions >>= and 
 return, as well as the functions in the list module. *)

(*waits until both the deferred x1 and the deferred x2 have been determined
  before returning a tuple of x1 and x2--based off both in Deferred.std*)
let my_both dx1 dx2 = dx1 >>= (fun x1 -> dx2 >>= fun x2 -> return (x1, x2))

(*similar to all in Std.Deferred*)
let my_all (l: 'a Deferred.t list) : 'a list Deferred.t = 
  let f de dacc = 
    my_both de dacc >>= fun (e,acc) -> 
    return (e::acc)
  in List.fold_right f l (return []) 

(*like the List module map, but allows the processing of each element to occur
  concurrently*)
let deferred_map l f = my_all (List.map f l)





