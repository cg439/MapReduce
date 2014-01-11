open Util
open Worker_manager

let mutex = Mutex.create()

let pushWork workQ pending : unit =  Mutex.lock mutex; 
                   Hashtbl.iter (fun a b -> Queue.add b workQ; 
                   Hashtbl.remove pending a) pending;
                   Mutex.unlock mutex

let map kv_pairs shared_data map_filename : (string * string) list = 
  let tpool = Thread_pool.create 100 in
  let resultsL = ref [] in
  let manager = Worker_manager.initialize_mappers map_filename shared_data in
  let run = ref 1 in
  let workQ = Queue.create() in
  let pending = Hashtbl.create 10 in
  let mapHelper kv worker : unit =
    Mutex.lock mutex;
    match (Worker_manager.map worker (fst kv) (snd kv)) with
    | Some(x) -> if Hashtbl.mem pending worker then
                 (Hashtbl.remove pending worker; 
		  List.iter (fun a -> resultsL:= a::!resultsL) x;
		  Worker_manager.push_worker manager worker; 
		  Mutex.unlock mutex)
	          else Worker_manager.push_worker manager worker; Mutex.unlock mutex
    | None -> Worker_manager.push_worker manager worker; Mutex.unlock mutex in
     List.iter (fun a -> Queue.add a workQ) kv_pairs;
       while (!run = 1) do
	 if not(Queue.is_empty workQ) then 
	 begin
          let worker = Worker_manager.pop_worker manager in
          let kv = Queue.pop workQ in
	  let callMap () = mapHelper kv worker in
          Mutex.lock mutex;
          Hashtbl.add pending worker kv;
	  Mutex.unlock mutex;
          Thread_pool.add_work callMap tpool;
         end
	 else
	  if(Hashtbl.length pending > 0) then pushWork workQ pending
	  else run := 0
       done;
       Thread_pool.destroy tpool;
       Worker_manager.clean_up_workers manager;
       !resultsL
  
let combine kv_pairs : (string * string list) list = 
   let results = Hashtbl.create 400 in
   List.iter(fun a -> if Hashtbl.mem results (fst a) then Hashtbl.replace results (fst a) ((snd a)::Hashtbl.find results (fst a))
       else Hashtbl.add results (fst a) ((snd a)::[])) kv_pairs;
   Hashtbl.fold (fun k v acc -> (k,v)::acc) results []
  
let reduce kvs_pairs shared_data reduce_filename : (string * string list) list =
  let tpool = Thread_pool.create 100 in
  let manager = Worker_manager.initialize_reducers reduce_filename shared_data in
  let resultsL = ref [] in
  let run = ref 1 in
  let workQ = Queue.create() in
  let pending = Hashtbl.create 10 in
  let reduceHelper kv worker : unit =
    Mutex.lock mutex;
    match (Worker_manager.reduce worker (fst kv) (snd kv)) with
    | Some(x) ->
	         if Hashtbl.mem pending worker then
                 (Hashtbl.remove pending worker; 
		  Worker_manager.push_worker manager worker;
		  resultsL:= (fst kv,x)::!resultsL; 
		  Mutex.unlock mutex)
	          else (Worker_manager.push_worker manager worker; 
			Mutex.unlock mutex)
    | None -> (Worker_manager.push_worker manager worker;
	      Mutex.unlock mutex) in 
     List.iter (fun a -> Queue.add a workQ) kvs_pairs;
       while (!run = 1) do
	 if not(Queue.is_empty workQ) then 
	 begin
          let worker = Worker_manager.pop_worker manager in
          let kv = Queue.pop workQ in
	  let callReduce () = reduceHelper kv worker in
          Mutex.lock mutex;
          Hashtbl.add pending worker kv;
	  Mutex.unlock mutex;
          Thread_pool.add_work callReduce tpool;
         end
	 else
	  if(Hashtbl.length pending > 0) then pushWork workQ pending
	  else run := 0
       done;
       Thread_pool.destroy tpool;
       Worker_manager.clean_up_workers manager;
       !resultsL

let map_reduce (app_name : string) (mapper : string) 
    (reducer : string) (filename : string) =
  let app_dir = Printf.sprintf "apps/%s/" app_name in
  let docs = load_documents filename in
  let titles = Hashtbl.create 16 in
  let add_document (d : document) : (string * string) =
    let id_s = string_of_int d.id in
    Hashtbl.add titles id_s d.title; (id_s, d.body) in
  let kv_pairs = List.map add_document docs in
  let mapped = map kv_pairs "" (app_dir ^ mapper ^ ".ml") in
  let combined = Util.print_map_results mapped; combine mapped in
  let reduced = Util.print_combine_results combined; reduce combined  "" (app_dir ^ reducer ^ ".ml") in
  (titles, reduced)
