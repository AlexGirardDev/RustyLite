# Rusty SQLite

A toy SQLite libary written in rust following [CodeCrafters](https://app.codecrafters.io) SQLite challenges. I made this mainly as a way to get a deeper understanding of how databases work, it also gave me another reason to write more Rust. 

This is written from scratch with the exception of a sql parser as I had just finished [Monkey Lang](https://github.com/AlexGirardDev/rusty-monkey-lang) and did't feel like writing another parser / lexer.

## Main features Implemented
- Read database schema
- Read data from multiple columns
- Full-Table scan row retrieval
- Indexed Select queries


## Running the Project
   ```bash
   cargo run sample.db "SELECT name, color FROM apples"
   cargo run superheroes.db "SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes'"
   ```
