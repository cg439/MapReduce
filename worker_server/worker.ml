open Protocol

let maptbl = Hashtbl.create 10
let redtbl = Hashtbl.create 10
let muMap = Mutex.create()
let muRed = Mutex.create()
let send_response client response =
  let success = Connection.output client response in
    (if not success then
      (Connection.close client;
       print_endline "Connection lost before response could be sent.")
    else ());
    success

let rec handle_request client =
  let pRes bool = if bool then handle_request client
                  else () in
  let ver id tbl mu = Mutex.lock mu;
                    if Hashtbl.mem tbl id
                    then let ans = true in
                    Mutex.unlock mu; ans
                    else let ans = false in
                    Mutex.unlock mu; ans in
  match Connection.input client with
    Some v ->
      begin
        match v with
        | InitMapper (source, shared_data) -> (match (Program.build source) with
                     | (Some id, str) -> Hashtbl.add maptbl id ""; 
                                         Program.write_shared_data id shared_data; 
                                         pRes(send_response client (Mapper(Some id, "")))
		     | (None, error) -> pRes(send_response client (Mapper(None, error))))
        | InitReducer source -> (match (Program.build source) with
	             | (Some id, str) -> Hashtbl.add redtbl id ""; 
                                         pRes(send_response client (Reducer(Some id, "")))
		     | (None, error) -> pRes(send_response client (Reducer (None, error))))
        | MapRequest (id, k, v) -> if (ver id maptbl muMap) then (match (Program.run id (k,v)) with
                                                                  | Some value -> pRes (send_response client (MapResults(id,value)))
								  | None -> pRes (send_response client (RuntimeError(id,"No output"))))
                                   else pRes (send_response client (InvalidWorker(id)))
        | ReduceRequest (id, k, v) -> if (ver id redtbl muRed) then (match (Program.run id (k,v)) with
	                                                            | Some value -> pRes (send_response client (ReduceResults(id,value)))
                                                                    | None -> pRes (send_response client (RuntimeError(id,"No output"))))
	                              else pRes(send_response client (InvalidWorker(id)))
          
      end
  | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."

