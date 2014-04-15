open Async.Std
open Async_unix

type filename = string

(******************************************************************************)
(** {2 The Inverted Index Job}                                                *)
(******************************************************************************)

module Job = struct
  type filename = string
  type word = string

  type input = filename
  type key = word
  type inter = filename
  type output = filename list

  let name = "index.job"

  let map (input : input): (key * inter) list Deferred.t =
    (*Can I use split_words this way?*)
    (Reader.file_contents input) >>= (fun txt ->
      let words = AppUtils.split_words txt in
        return (List.map (fun x -> (x, input)) words))

  let reduce (key, inters) : output Deferred.t =
    (*get rid of duplicates*)
    let rec reduce_helper inters : inter list =
      match inters with
      | [] -> []
      | hd::tl -> hd::(reduce_helper (List.filter ((=) hd) tl))
    in return (reduce_helper inters)
end

(* register the job *)
let () = MapReduce.register_job (module Job)


(******************************************************************************)
(** {2 The Inverted Index App}                                                *)
(******************************************************************************)

module App  = struct

  let name = "index"

  (** Print out all of the documents associated with each word *)
  let output results =
    let print (word, documents) =
      print_endline (word^":");
      List.iter (fun doc -> print_endline ("    "^doc)) documents
    in

    let sorted = List.sort compare results in
    List.iter print sorted


  (** for each line f in the master list, output a pair containing the filename
      f and the contents of the file named by f.  *)
  let read (master_file : filename) : (filename * string) list Deferred.t =
    Reader.file_lines master_file >>= fun filenames ->

    Deferred.List.map filenames (fun filename ->
      Reader.file_contents filename >>= fun contents ->
      return (filename, contents)
    )

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    (** The input should be a single file name.  The named file should contain
        a list of files to index. *)
    let main (args: filename list) =
      match args with
      | [arg] -> (Reader.file_lines arg) >>= MR.map_reduce >>| output
      | [] -> failwith "no files provided"
      | _ -> failwith "too many files provided; supply only 1 file"
  end
end 

(* register the App *)
let () = MapReduce.register_app (module App)

