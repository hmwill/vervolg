// MIT License
//
// Copyright (c) 2018 Hans-Martin Will
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

extern crate tabwriter;
extern crate vervolg;

use std::io::prelude::*;
use std::io;

use tabwriter::TabWriter;

fn main() {
    let mut input_line = String::new();
    let mut session = vervolg::session::Session::new();

    loop {
        print!("> ");
        let _ = io::stdout().flush(); // Make sure the '>' prints

        // Read in a string from stdin
        io::stdin().read_line(&mut input_line).ok().expect("The read line failed O:");
        
        // If 'exit' break out of the loop.
        match input_line.trim() {
            "EXIT" => break, //This could interfere with some parsers, so be careful
            line => {
                let mut evaluator = vervolg::eval::Evaluator::new(&mut session);
                let result = evaluator.eval(&line.to_string());

                match result {
                    Ok(mut rowset) => {
                        let stdout = io::stdout();
                        let mut handle = stdout.lock();
                        let mut tw = TabWriter::new(handle);

                        while let Some(row) = rowset.next() {
                            if row.is_ok() {
                                for field in row.unwrap().iter() {
                                    let variable: &str = field;
                                    write!(tw, "{}\t", variable).unwrap();
                                }

                                write!(tw, "\n").unwrap();
                            }
                        }

                        tw.flush().unwrap();
                    },
                    Err(err) => {
                        println!("{}", err);
                    }
                }
            }
        }
        
        // Clear the input line so we get fresh input
        input_line.clear();
    }

    println!("Bye...");
}