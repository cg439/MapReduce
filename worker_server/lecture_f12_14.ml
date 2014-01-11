(* recursion using mutable references *)
type 'a stream = 
  Nil | Cons of 'a * (unit -> 'a stream)

let hd (s: 'a stream) : 'a = 
  match s with 
    Nil -> failwith "hd" 
  | Cons(h,_) -> h

let tl (s: 'a stream) : 'a stream = 
  match s with 
    Nil -> failwith "hd" 
  | Cons(_,t) -> t ()

let from = 
  let r = ref (fun _ -> Nil) in 
  let g (n:int) = Cons(n, fun () -> !r (n + 1)) in 
  r := g;
  !r 

(* hashtables using mutable arrays *)
type ('a,'b) hashtbl = 
  { items : int ref;
    table : ((('a * 'b) list) array) ref
  }
    
let hash h k = 
  Hashtbl.hash k mod Array.length !(h.table)

let create n : ('a,'b) hashtbl = 
  { items = ref 0; 
    table = ref (Array.make n []) }

let find h k = 
  let i = hash h k in
  try Some (List.assoc k !(h.table).(i)) 
  with Not_found -> None

let rec resize h =
  let n = Array.length !(h.table) in
  let h' = create (2 * n) in
  for i = 0 to n - 1 do
    List.iter
      (fun (k,v) -> add h' k v) 
      !(h.table).(i)
  done;
  h.table := !(h'.table)

and add h k v = 
  if !(h.items) >= Array.length !(h.table) * 2 then 
    resize h;
  let i = hash h k in 
  !(h.table).(i) <- (k,v)::!(h.table).(i);
  h.items := !(h.items) + 1

let remove h k = 
  let i = hash h k in 
  let rec pop acc l = 
    match l with 
    | [] -> (false,List.rev acc)
    | (k',v)::t -> if k' = k then (true,List.rev acc @ t) else pop ((k',v)::acc) t in 
  let b,l' = pop [] !(h.table).(i) in 
  !(h.table).(i) <- l';
  if b then h.items := !(h.items) - 1 else ()

(* memoizing fib *)  
let rec fib n = 
  if n < 2 then 1 else fib (n-1) + fib (n-2)

let fibm n = 
  let memo = create n in 
  let rec f n = 
    match find memo n with 
    | Some result -> result
    | None -> 
      let result = if n < 2 then 1 else f (n-1) + f (n-2) in 
      add memo n result;
      result in 
  f n

(* memoizing line breaking *)
let cube x = x * x * x

let big = 10000

let ws1 = ["The"; "key"; "observation"; "is"; "that"; "in"; "the"; 
     "optimal"; "formatting"; "of"; "a"; "paragraph"; "of"; 
     "text,"; "the"; "formatting"; "of"; "the"; "text"; "past"; 
     "any"; "given"; "point"; "is"; "the"; "optimal"; "formatting";
     "of"; "just"; "that"; "text,"; "given"; "that"; "its"; "first";
     "character"; "starts"; "at"; "the"; "column"; "position"; 
     "where"; "the"; "prior"; "formatted"; "text"; "ends."; "Thus,";
     "the"; "formatting"; "problem"; "has"; "optimal"; "substructure"; 
     "when"; "cast"; "in"; "this"; "way."]

let ws2 = ["The"; "key"; "observation"; "is"; "that"; "in"; "the"; 
     "optimal"; "formatting"; "of"; "a"; "paragraph"; "of"; 
     "text,"; "the"; "formatting"; "of"; "the"; "text"; "past";
     "any"; "given"; "point"]

let linebreak (words:string list) (target:int) : string list = 
  let rec lb (clen:int) (words:string list) : string list * int = 
    match words with 
    | [] -> 
      ([""],0)
    | word::rest -> 
      let wlen = String.length word in 
      let clen' = if clen = 0 then wlen else clen + 1 + wlen in 
      let (l',c1') = lb 0 rest in 
      let c1 = c1' + cube (target - clen') in 
      if clen' <= target then 
        let (h2::t2,c2) = lb clen' rest in 
        if c1 < c2 then (word::l',c1) 
        else((if h2 = "" then word else word ^ " " ^ h2)::t2, c2)
      else (word::l',big) in 
  let (result,cost) = lb 0 words in 
  result

let linebreakm (words:string list) (target:int) : string list = 
  let memo = create (List.length words + 1) in 
  let rec lb_mem (words:string list) : string list * int = 
    let n = List.length words in 
    match find memo n with 
    | Some result -> result
    | None -> 
      let result = lb 0 words in 
      add memo n result;
      result
  and lb (clen:int) (words:string list) : string list * int = 
    match words with 
    | [] -> 
      ([""],0)
    | word::rest -> 
      let wlen = String.length word in 
      let clen' = if clen = 0 then wlen else clen + 1 + wlen in 
      let (l',c1') = lb_mem rest in 
      let c1 = c1' + cube (target - clen') in 
      if clen' <= target then 
        let (h2::t2,c2) = lb clen' rest in 
        if c1 < c2 then (word::l',c1) 
        else((if h2 = "" then word else word ^ " " ^ h2)::t2, c2)
      else (word::l',big) in 
  let (result,cost) = lb 0 words in 
  result
        
let pretty_print l = List.iter (fun x -> print_string x; print_newline ()) l

(* generic memoization *)
let memoize f = 
  let memo = create 17 in 
  (fun x -> 
    match find memo x with 
    | Some result -> result
    | None -> 
      let result = f x in 
      add memo x result;
      result)
    
(* fixed points *)
let t_fib g n = 
  if n < 2 then 1 else g (n-1) + g (n-2)

let fix t = 
  let rec g x = t g x in 
  g 

(* generic recursive memoization *)
let fix_memo t = 
  let memo = create 17 in 
  let rec g x = 
    match find memo x with 
    | Some result -> result
    | None -> 
      let result = t g x in 
      add memo x result;
      result in 
  g
      
