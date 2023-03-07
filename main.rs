#[macro_use] extern crate scan_fmt;
use std::io::{self, Write};
use std::io::prelude::*;
use std::fs::OpenOptions;
use std::process;
use std::cmp;
use std::mem;
use std::convert::TryInto;
use std::str;
use std::fs::File;
use std::io::SeekFrom;
use std::env;

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

const ID_SIZE: usize = mem::size_of::<i32>();
const USERNAME_SIZE: usize = mem::size_of::<[u8; COLUMN_USERNAME_SIZE]>();
const EMAIL_SIZE: usize = mem::size_of::<[u8; COLUMN_EMAIL_SIZE]>();

const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;

const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

#[derive(Debug)]
enum StatementType {
    Insert,
    Select,
}

#[derive(Debug)]
enum PrepareError {
    UnrecognizedStatement,
    SyntaxError,
    StringTooLong,
    NegativeId,
}

#[derive(Debug)]
enum PagerError {
    PageNumberOutOfBounds,
    EmptyPageFlush,
}

#[derive(Debug)]
#[repr(C)]
struct Row {
    id: i32,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE],
}

fn str_from_array(arr: &[u8]) -> &str {
    let null_pos = arr.iter().position(|&c| c == b'\0').unwrap_or(arr.len());
    str::from_utf8(&arr[..null_pos]).unwrap()
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
        buffer[USERNAME_OFFSET..USERNAME_OFFSET+USERNAME_SIZE]
            .copy_from_slice(&self.username);
        buffer[EMAIL_OFFSET..EMAIL_OFFSET+EMAIL_SIZE].copy_from_slice(&self.email);
    }

    fn print(&self) {
        let username_str = str_from_array(&self.username);
        let email_str = str_from_array(&self.email);
        println!("({}, {}, {})", self.id, username_str, email_str);
    }
}


#[derive(Debug)]
struct Statement {
    statement_type: StatementType,
    row_to_insert: Option<Row>, // only used by insert statement
}

impl Statement {
    fn prepare_insert(statement_text: &str) -> Result<Self, PrepareError> {
        let scan_result = scan_fmt!(
            statement_text, "insert {d} {} {}", i32, String, String
        );
        match scan_result {
            Ok((id, username, email)) => {
                if id < 0 {
                    return Err(PrepareError::NegativeId)
                }
                if username.len() > COLUMN_USERNAME_SIZE {
                    return Err(PrepareError::StringTooLong)
                }

                if email.len() > COLUMN_EMAIL_SIZE {
                    return Err(PrepareError::StringTooLong)
                }

                let mut username_bytes = [0u8; COLUMN_USERNAME_SIZE];
                let mut email_bytes = [0u8; COLUMN_EMAIL_SIZE];

                username_bytes[..cmp::min(username.len(), COLUMN_USERNAME_SIZE)]
                    .copy_from_slice(username.as_bytes());
                email_bytes[..cmp::min(email.len(), COLUMN_EMAIL_SIZE)]
                    .copy_from_slice(email.as_bytes());

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

    fn prepare(statement_text: &str) -> Result<Self, PrepareError> {
        if statement_text.starts_with("insert") {
            return Self::prepare_insert(statement_text);
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

#[derive(Debug)]
#[repr(C)]
struct Pager {
    file: File,
    file_length: u64,
    pages: [Option<Box<[u8; PAGE_SIZE]>>; TABLE_MAX_PAGES]
}

impl Pager {
    fn open(filename: &str) -> Self {
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(filename)
            .unwrap();
        let file_length = file.seek(SeekFrom::End(0)).unwrap();

        const INIT: Option<Box<[u8; PAGE_SIZE]>> = None;
        Self {
            file,
            file_length,
            pages: [INIT; TABLE_MAX_PAGES],
        }
    }

    fn get_page(&mut self, page_num: usize) -> Result<&mut [u8], PagerError> {
        if page_num > TABLE_MAX_PAGES {
            return Err(PagerError::PageNumberOutOfBounds);
        }

        if let None = self.pages[page_num] {
            let mut page = Box::new([0u8; PAGE_SIZE]);
            let mut num_pages = self.file_length as usize / PAGE_SIZE;

            if self.file_length % PAGE_SIZE as u64 != 0 {
                num_pages += 1;
            }

            if page_num <= num_pages {
                self.file.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64)).unwrap();
                self.file.read(&mut *page).unwrap();
            }

            self.pages[page_num].replace(page);
        }

        Ok(&mut self.pages[page_num].as_mut().unwrap()[..])
    }

    fn flush(&mut self, page_num: usize, size: usize) -> Result<(), PagerError> {
        if let None = self.pages[page_num] {
            return Err(PagerError::EmptyPageFlush);
        }

        self.file.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64)).unwrap();
        self.file.write(
            self.pages[page_num].as_ref().unwrap()[..size].as_ref()
        ).unwrap();

        Ok(())
    }
}

#[derive(Debug)]
#[repr(C)]
struct Table {
    num_rows: usize,
    pager: Pager,
}

impl Table {
    fn new(filename: &str) -> Self {
        let pager = Pager::open(filename);
        let num_rows = pager.file_length as usize / ROW_SIZE;

        Self {
            num_rows,
            pager,
        }
    }

    fn row_slot(&mut self, row_num: usize) -> &mut [u8] {
        let page_num: usize = row_num / ROWS_PER_PAGE;

        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        let page = self.pager.get_page(page_num).unwrap();
        &mut page[byte_offset..byte_offset+ROW_SIZE]
    }

    fn close(&mut self) {
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;

        for i in 0..num_full_pages {
            match self.pager.pages[i] {
                Some(_) => self.pager.flush(i, ROW_SIZE).unwrap(),
                None => continue
            };
        }

        let num_additional_rows = self.num_rows % ROWS_PER_PAGE;
        if num_additional_rows > 0 {
            let page_num = num_full_pages;
            if let Some(_) = self.pager.pages[page_num] {
                self.pager.flush(page_num, num_additional_rows * ROW_SIZE).unwrap();
            }
        }
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

    let row_to_insert = statement.row_to_insert.as_ref().unwrap();
    row_to_insert.serialize(table.row_slot(table.num_rows));
    table.num_rows += 1;

    Ok(())
}

#[allow(unused_variables)]
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

fn do_meta_command(command: &str, table: &mut Table) -> Result<(), ()> {
    match command {
        ".exit" => {
            table.close();
            process::exit(0);
        },
        _ => return Err(())
    };
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let filename;
    if args.len() > 1 {
        filename = args[1].as_str();
    } else {
        filename = "db.dat";
    }

    let mut table = Table::new(&filename);

    loop {
        let input = read_input("db > ")?;

        if input.starts_with(".") {
            match do_meta_command(&input, &mut table) {
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
                            ExecuteError::TableFull => {
                                println!("Error: Table full.");
                                continue;
                            }
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
                },
                PrepareError::StringTooLong => {
                    println!("String is too long.");
                    continue;
                },
                PrepareError::NegativeId => {
                    println!("ID must be positive.");
                    continue;
                }
            }
        }
    }
}

