open Async.Std
open Async_unix

(******************************************************************************)
(** input and output types                                                    *)
(******************************************************************************)

type id = int
type dna_type = Read | Ref

type sequence = {
  id   : id;
  kind : dna_type;
  data : string;
}

(** Indicates a matching subsequence of the given read and reference *)
type result = {
  length   : int;

  read     : id;
  read_off : int;

  ref      : id;
  ref_off  : int;
}

(******************************************************************************)
(** file reading and writing                                                  *)
(******************************************************************************)

(** Convert a line into a sequence *)
let read_sequence line = match Str.split (Str.regexp "@") line with
  | [id; "READ"; seq] -> {id=int_of_string id; kind=Read; data=seq}
  | [id; "REF";  seq] -> {id=int_of_string id; kind=Ref;  data=seq}
  | _ -> failwith "malformed input"

(** Read in the input data *)
let read_files filenames : sequence list Deferred.t =
  if filenames = [] then failwith "No files supplied"
  else
    Deferred.List.map filenames Reader.file_lines
      >>| List.flatten
      >>| List.map read_sequence


(** Print out a single match *)
let print_result result =
  printf "read %i [%i-%i] matches reference %i [%i-%i]\n"
         result.read result.read_off (result.read_off + result.length - 1)
         result.ref  result.ref_off  (result.ref_off  + result.length - 1)

(** Write out the output data *)
let print_results results : unit =
  List.iter print_result results

(******************************************************************************)
(** Dna sequencing jobs                                                       *)
(******************************************************************************)

module Job1 = struct
  type offset = int
  type kmer = string

  type input = sequence
  type key = kmer 
  type inter = (sequence * offset)
  type output = result list

  let name = "dna.job1"

  let k = 10

  let pull_kmer (offset: int) (k: int) (seq: sequence) : (key * inter) =
    (String.sub (seq.data) offset k, (seq, offset))

  let map (input: input) : (key * inter) list Deferred.t =
    let rec extract_kmers (start: int) (seq: sequence) =
      if start = ((String.length (seq.data)) -k +1) then []
      else (pull_kmer start k seq)::(extract_kmers (start + 1) seq)
    in return (extract_kmers 0 input)

  let reduce (key, inters) : output Deferred.t =
    let p ((s: sequence), (o: offset)) = (s.kind == Ref) in
    let (refs, reads) = List.partition p inters in
    let f1 (ref_s, ref_o) = 
      let f2 (read_s, read_o) = {length=k; read=read_s.id; read_off=read_o;
                                              ref=ref_s.id; ref_off= ref_o}
      in List.map f2 reads
    in return (List.flatten (List.map f1 refs))
end

let () = MapReduce.register_job (module Job1)


module Job2 = struct

  type kmer = string
  type input = kmer * (result list)
  type key = (id * id)
  type inter = result
  type output = result list

  type comp = Lt | Eq | Gt

  let name = "dna.job2"

  let map ((kmer: kmer), (ilst: result list)) : (key * inter) list Deferred.t =
    let f input = ((input.read, input.ref), input) in
    return (List.map f ilst)

  let reduce (key, inters) : output Deferred.t =
    let cmp (res1: result) (res2: result) = 
      let cmp_hlp (res1: result) (res2: result) =
      let dif1= (res1.ref_off - res1.read_off)
      and dif2 = (res2.ref_off - res2.read_off) in
        if dif1 < dif2 then Lt
        else if dif1 > dif2 then Gt
          else if res1.ref_off < res2.ref_off then Lt
            else if res1.ref_off > res2.ref_off then Gt
              else Eq
      in match (cmp_hlp res1 res2) with
      | Lt -> -1
      | Eq -> 0
      | Gt -> 1
    in 
    let sorted_inters = List.sort cmp inters in
    let f (acc: result list) (e: result) : result list =
      match acc with
      | [] -> [e]
      | hd::tl ->
        begin
          if (e.ref_off <= (hd.ref_off+hd.length))
            &&((e.read_off <= (hd.read_off+hd.length)))
            then {length = (e.ref_off + e.length - hd.ref_off);
                 read = hd.read;
                 read_off = hd.read_off;
                 ref = hd.ref;
                 ref_off = hd.ref_off}::tl
          else e::acc
        end
    in return (List.fold_left f [] sorted_inters)
end

let () = MapReduce.register_job (module Job2)



module App  = struct

  let name = "dna"

  module Make (Controller : MapReduce.Controller) = struct
    module MR1 = Controller(Job1)
    module MR2 = Controller(Job2)

    let run (input : sequence list) : result list Deferred.t =
      (MR1.map_reduce input)
      >>= MR2.map_reduce
      >>| (fun lst -> List.flatten (List.map (fun (k,o) -> o) lst))


    let main args =
      read_files args
        >>= run
        >>| print_results
  end
end

let () = MapReduce.register_app (module App)

