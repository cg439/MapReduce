let (key, value) = Program.get_input() in
Program.set_output (List.fold_left (fun acc k -> (k, key)::acc) []
                   (Util.split_words value))
