let (key, values) = Program.get_input() in
let remove_dups lst =
 List.fold_left (fun acc z -> if List.mem z acc then acc else z::acc) [] lst in
let compare a b = let intA = int_of_string a in
                  let intB = int_of_string b in
                  if intA = intB then 0
                  else if intA > intB then 1
                  else -1 in
    Program.set_output (List.sort compare (remove_dups values))
