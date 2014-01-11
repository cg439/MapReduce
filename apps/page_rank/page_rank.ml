open Util

let main (args : string array) : unit = 
  if Array.length args < 3 then
    Printf.printf "Usage: page_rank <filename> <numiterations>"
  else
    failwith "done"
