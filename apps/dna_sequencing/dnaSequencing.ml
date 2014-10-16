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

  let k = 10 (*specifies the size of the k-mers*)

  (*produces a k-character sub-sequence of [seq] starting at index [i], along 
    with the tuple ([seq], [i] (so that, later on, we can figure out where the 
    k-mer came from) *)
  let get_kmer (seq: sequence) (index: int) : (key * inter) = 
    String.sub seq.data index k, (seq, i)

  (*moves through the input sequence, producing a list of every contiguous k-mer
    in the input (along with tags for each k-mer)*)
  let map (input: input) : (key * inter) list Deferred.t = 
    let rec traverse i = 
      if i = (String.lenth (seq.data)) - k + 1 then []
      else (get_kmer input i)::(traverse (i+1))
    in traverse 0

  (*produces the cartesian product of l1 and l2 (albeit in an order that's the
    reverse of the usually ordering); tail-recursive*)
  let cart_prod l1 l2 = 
    let f acc e = (List.fold_left (fun acc e' -> (e, e')::acc) acc l2) in 
    List.fold_left f [] l1

  (*takes a k-mer along with a list of sequences and offsets, and returns a list
    of possible matches between reads and refs. 
    Note: the sequences in [inters] could contain multiple reads and refs, so 
    for any given read/ref, it's necessary to create a match between that
    read/ref and all the refs/reads in inters. Hence the need to take the 
    cartesion product of the set of refs and the set of reads. *)
  let reduce (key, inters) : output Deferred.t = 
    let is_ref (s, o) = (s.kind = Ref) in
    let (refs, reads) = List.partion is_ref inters in
    let prod = cart_prod refs reads in
    let f ((ref_s, ref_o), (read_s, read_o)) = 
      {length = k; 
       read = read_s.id;
       read_off = read_o;
       ref = ref_s.id;
       ref_off = ref_o}
    in List.map f prod


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

  (*takes in a kmer and a set of matches produced by the first reduce phase
    and labels each match with the ref id and read id found in the match*)
  let map ((k: kmer), (matches: result list)) : (key * inter) list Deferred.t =
    let f match = ((match.read, match.ref), match) in
    return (List.map f matches)

  (*thought: if two matches haves an overlap, the the first result (sorted by
    the location of the read_offset), should also have an earlier ref_offset*)
  (*I ought to handle corner cases more carefully*)

  (*checks for an overlap between the matches
    note: assumes that r1.read_off =< r2.read_off*)
  let overlap (r1: result) (r2: result) : bool =
    let read_overlap = r2.read_off < r1.read_off + r1.length
    and ref_overlap = 
      let (first, second) =
        if r1.ref_off < r2.ref_off then (r1,r2) else (r2,r1)
      in first.ref_off < r1.ref_off + r1.length
    in read_overlap && ref_overlap

  (*takes to results and merges them into a single contiguous result*)
  let merge (r1: result) (r2: result) : result 

  (*combines adjacent kmer matches*)
  let reduce (key, inters) : output Deferred.t = 
    let cmp res1 res2 = compare res1.read_off res2.read_off in
    let 

  let reduce (key, inters) : output Deferred.t = 
    let cmp (res1: result) (res2: result) = 
      compare (res1.read_off, res1.ref_off) (res2.read_off, res2.ref_off)
    in 
    let srtd_inters = List.sort cmp inters in
    let f acc res = 
      let overlapping_rs = List.partition ()

  let reduce (key, inters) : output Deferred.t = 
    let cmp (res1: result) (res2: result) = 
      let dif1 = (res1.ref_off - res1.read_off) 
      and dif2 = (res2.ref_off - res2.read_off) in
      compare (dif1, res1.ref_off) (dif2, res2.ref_off)
    in
    let sorted_inters = List.sort cmp inters in 


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

