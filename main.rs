#[macro_use] extern crate scan_fmt;
use std::io::{self, Write};
use std::process;
use std::cmp;
use std::mem;
use std::convert::TryInto;
use std::str;

#[derive(Debug)]
enum StatementType {
    Insert,
    Select,
}

#[derive(Debug)]
enum PrepareError {
    UnrecognizedStatement,
    SyntaxError,
}

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

#[derive(Debug)]
#[repr(C)]
struct Row {
    id: i32,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE],
}

#[derive(Debug)]
#[repr(C)]
struct Statement {
    statement_type: StatementType,
    row_to_insert: Option<Row>, // only used by insert statement
}

// field sizes
const ID_SIZE: usize = mem::size_of::<i32>();
const USERNAME_SIZE: usize = mem::size_of::<[u8; COLUMN_USERNAME_SIZE]>();
const EMAIL_SIZE: usize = mem::size_of::<[u8; COLUMN_EMAIL_SIZE]>();
// fields offsets
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
// row size
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

impl Statement {
    fn prepare(statement_text: &str) -> Result<Self, PrepareError> {
        if statement_text.starts_with("insert") {
            let scan_result = scan_fmt!(
                statement_text, "insert {d} {} {}", i32, String, String
            );
            match scan_result {
                Ok((id, username, email)) => {
                    let mut username_bytes = [0u8; COLUMN_USERNAME_SIZE];
                    let mut email_bytes = [0u8; COLUMN_EMAIL_SIZE];
                    username_bytes[
                        ..cmp::min(username.len(), COLUMN_USERNAME_SIZE)
                    ].copy_from_slice(username.as_bytes());
                    email_bytes[
                        ..cmp::min(email.len(), COLUMN_EMAIL_SIZE)
                    ].copy_from_slice(email.as_bytes());

                    return Ok(Self {
                        statement_type: StatementType::Insert,
                        row_to_insert: Some(Row {
                            id,
                            username: username_bytes,
                            email: email_bytes,
                        })
                    });
                },
                Err(_) => return Err(PrepareError::SyntaxError)
            };
        }

        if statement_text.starts_with("select") {
            return Ok(Self {
                statement_type: StatementType::Select,
                row_to_insert: None
            });
        }

        Err(PrepareError::UnrecognizedStatement)
    }
}

impl Row {
    fn deserialize(bytes: &[u8]) -> Self {
        let mut username = [0u8; COLUMN_USERNAME_SIZE];
        let mut email = [0u8; COLUMN_EMAIL_SIZE];
        let id = i32::from_le_bytes(
            bytes[ID_OFFSET..ID_OFFSET+ID_SIZE].try_into().unwrap()
        );
        username.copy_from_slice(&bytes[USERNAME_OFFSET..USERNAME_OFFSET+USERNAME_SIZE]);
        email.copy_from_slice(&bytes[EMAIL_OFFSET..EMAIL_OFFSET+EMAIL_SIZE]);
        Self { id, username, email }
    }

    fn serialize(&self, buffer: &mut [u8]) {
        buffer[ID_OFFSET..ID_OFFSET+ID_SIZE].copy_from_slice(&self.id.to_le_bytes());
        buffer[USERNAME_OFFSET..USERNAME_OFFSET+USERNAME_SIZE].copy_from_slice(&self.username);
        buffer[EMAIL_OFFSET..EMAIL_OFFSET+EMAIL_SIZE].copy_from_slice(&self.email);
    }

    fn print(&self) {
        let username_str = str::from_utf8(&self.username).unwrap();
        let email_str = str::from_utf8(&self.email).unwrap();
        println!("({}, {}, {})", self.id, username_str, email_str);
    }
}

#[derive(Debug)]
struct Table {
    num_rows: usize,
    pages: [Option<Box<[u8; PAGE_SIZE]>>; TABLE_MAX_PAGES]
}

impl Table {
    fn new() -> Self {
        const INIT: Option<Box<[u8; PAGE_SIZE]>> = None;
        Self {
            num_rows: 0,
            pages: [INIT; TABLE_MAX_PAGES],
        }
    }

    fn row_slot(&mut self, row_num: usize) -> &mut [u8] {
        let page_num: usize = row_num / ROWS_PER_PAGE;

        if let None = self.pages[page_num] {
            self.pages[page_num].replace(Box::new([0u8; PAGE_SIZE]));
        }

        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        &mut self.pages[page_num].as_mut().unwrap()[byte_offset..byte_offset+ROW_SIZE]
    }
}

#[derive(Debug)]
enum ExecuteError {
    TableFull,
}

fn execute_insert(statement: &Statement, table: &mut Table) -> Result<(), ExecuteError> {
    if table.num_rows >= TABLE_MAX_ROWS {
        return Err(ExecuteError::TableFull);
    }

    let row_to_insert = &(statement.row_to_insert.as_ref().unwrap());
    row_to_insert.serialize(table.row_slot(table.num_rows));
    table.num_rows += 1;

    Ok(())
}

fn execute_select(statement: &Statement, table: &mut Table) -> Result<(), ExecuteError> {
    for i in 0..table.num_rows {
        let row = Row::deserialize(table.row_slot(i));
        row.print();
    }
    Ok(())
}

fn execute_statement(
    statement: &Statement, table: &mut Table
) -> Result<(), ExecuteError> {
    return match statement.statement_type {
        StatementType::Insert => execute_insert(statement, table),
        StatementType::Select => execute_select(statement, table),
    }
}

fn read_input(prompt: &str) -> io::Result<String> {
    print!("{}", prompt);
    io::stdout().flush()?;

    let mut input_buffer = String::new();
    io::stdin().read_line(&mut input_buffer)?;

    Ok(input_buffer.trim().to_string())
}

fn do_meta_command(command: &str) -> Result<(), ()> {
    match command {
        ".exit" => process::exit(0),
        _ => return Err(())
    };
}

fn main() -> io::Result<()> {
    let mut table = Table::new();

    loop {
        let input = read_input("db > ")?;

        if input.is_empty() {
            break;
        }

        if input.starts_with(".") {
            match do_meta_command(&input) {
                Ok(()) => continue,
                Err(()) => {
                    println!("Unrecognized command: {}", input);
                    continue;
                }
            }
        }

        match Statement::prepare(&input) {
            Ok(statement) => {
                match execute_statement(&statement, &mut table) {
                    Ok(()) => println!("Executed."),
                    Err(execute_error) => {
                        match execute_error {
                            ExecuteError::TableFull => println!("Error: Table full.")
                        }
                    }
                }
            },
            Err(prepare_error) => match prepare_error {
                PrepareError::UnrecognizedStatement => {
                    println!("Unrecognized keyword at start of '{}'", input);
                    continue;
                },
                PrepareError::SyntaxError => {
                    println!("Syntax error. Count not parse statement.");
                    continue;
                }
            }
        }
    }

    Ok(())
}

