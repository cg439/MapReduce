let (key, value) = Program.get_input() in
  let links = 
  Hashtable.find (Util.unmarshal (Program.get_shared_data()))
     (Util.unmarshal key) in
  Program.set_output 
   (List.map (fun x -> (Util.marshal x, Util.marshal rank)) links)
