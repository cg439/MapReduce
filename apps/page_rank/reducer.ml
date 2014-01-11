let (key,values) = Program.get_input() in
 let floatList = Util.unmarshal values in
 let sum = List.fold_left (+.) 0.0 floatList in
  Program.set_output ([Util.marshal sum])  
