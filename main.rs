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

// Common Node Header Layout
const NODE_TYPE_SIZE: usize = mem::size_of::<u8>();
const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = mem::size_of::<u8>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = mem::size_of::<u32>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE: usize =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf Node Header Layout
const LEAF_NODE_NUM_CELLS_SIZE: usize = mem::size_of::<u32>();
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Leaf Node Body Layout
const LEAF_NODE_KEY_SIZE: usize = mem::size_of::<u32>();
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

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

enum NodeType {
    Internal,
    Leaf,
}

#[derive(Debug)]
#[repr(C)]
struct Row {
    id: u32,
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
        let id = u32::from_le_bytes(
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
                        id: id as u32,
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
    num_pages: u32,
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

        if file_length as usize % PAGE_SIZE != 0 {
            panic!("Db file is not a whole number of pages. Corrupt file.");
        }

        const INIT: Option<Box<[u8; PAGE_SIZE]>> = None;
        Self {
            file,
            file_length,
            num_pages: (file_length / PAGE_SIZE as u64) as u32,
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

            if page_num as u32 >= self.num_pages {
                self.num_pages = page_num as u32 + 1;
            }
        }

        Ok(&mut self.pages[page_num].as_mut().unwrap()[..])
    }

    fn flush(&mut self, page_num: usize) -> Result<(), PagerError> {
        if let None = self.pages[page_num] {
            return Err(PagerError::EmptyPageFlush);
        }

        self.file.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64)).unwrap();
        self.file.write(
            self.pages[page_num].as_ref().unwrap().as_ref()
        ).unwrap();

        Ok(())
    }
}

#[derive(Debug)]
#[repr(C)]
struct Table {
    root_page_num: u32,
    pager: Pager,
}

impl Table {
    fn new(filename: &str) -> Self {
        let mut pager = Pager::open(filename);

        if pager.num_pages == 0 {
            let root_node = pager.get_page(0).unwrap();
            initialize_leaf_node(root_node);
        }

        Self { root_page_num: 0, pager }
    }

    fn close(&mut self) {
        for i in 0..self.pager.num_pages {
            match self.pager.pages[i as usize] {
                Some(_) => self.pager.flush(i as usize).unwrap(),
                None => continue
            };
        }
    }
}

fn leaf_node_num_cells(node: &[u8]) -> u32 {
    let start = LEAF_NODE_NUM_CELLS_OFFSET;
    let end = start + LEAF_NODE_NUM_CELLS_SIZE;
    u32::from_le_bytes(node[start..end].try_into().unwrap())
}

fn leaf_node_set_num_cells(node: &mut [u8], num_cells: u32) {
    let start = LEAF_NODE_NUM_CELLS_OFFSET;
    let end = start + LEAF_NODE_NUM_CELLS_SIZE;
    node[start..end].copy_from_slice(&num_cells.to_le_bytes());
}

fn leaf_node_cell(node: &mut [u8], cell_num: u32) -> &mut [u8] {
    let start = LEAF_NODE_HEADER_SIZE + cell_num as usize * LEAF_NODE_CELL_SIZE;
    let end = start + LEAF_NODE_CELL_SIZE;
    &mut node[start..end]
}

fn copy_within_a_slice<T: Clone>(v: &mut [T], from: usize, to: usize, len: usize) {
    if from > to {
        let (dst, src) = v.split_at_mut(from);
        dst[to..to + len].clone_from_slice(&src[..len]);
    } else {
        let (src, dst) = v.split_at_mut(to);
        dst[..len].clone_from_slice(&src[from..from + len]);
    }
}

fn shift_cell_right(node: &mut [u8], cell_num: u32) {
    let start1 = LEAF_NODE_HEADER_SIZE + cell_num as usize * LEAF_NODE_CELL_SIZE;
    let start2 = LEAF_NODE_HEADER_SIZE + (cell_num-1) as usize * LEAF_NODE_CELL_SIZE;

    copy_within_a_slice(node, start2, start1, LEAF_NODE_CELL_SIZE);
}

fn leaf_node_key(node: &mut [u8], cell_num: u32) -> u32 {
    let cell = leaf_node_cell(node, cell_num);
    u32::from_le_bytes(cell[..LEAF_NODE_KEY_SIZE].try_into().unwrap())
}

fn leaf_node_set_key(node: &mut [u8], cell_num: u32, key: u32) {
    let cell = leaf_node_cell(node, cell_num);
    cell[..LEAF_NODE_KEY_SIZE].copy_from_slice(&key.to_le_bytes());
}

fn leaf_node_value(node: &mut [u8], cell_num: u32) -> &mut [u8] {
    let cell = leaf_node_cell(node, cell_num);
    &mut cell[LEAF_NODE_KEY_SIZE..]
}

fn initialize_leaf_node(_node: &mut [u8]) {}

fn leaf_node_insert(cursor: &mut Cursor, key: u32, value: &Row) {
    let node = cursor.table.pager.get_page(cursor.page_num).unwrap();

    let num_cells = leaf_node_num_cells(node);
    if num_cells as usize >= LEAF_NODE_MAX_CELLS {
        panic!("Need to implement splitting a leaf node.")
    }

    if cursor.cell_num < num_cells as usize {
        for i in num_cells..cursor.cell_num as u32 {
            shift_cell_right(node, i);
        }
    }

    leaf_node_set_num_cells(node, num_cells + 1);
    leaf_node_set_key(node, cursor.cell_num as u32, key);
    value.serialize(leaf_node_value(node, cursor.cell_num as u32));
}

struct Cursor<'a> {
    table: &'a mut Table,
    page_num: usize,
    cell_num: usize,
    end_of_table: bool,
}

impl <'a> Cursor<'a> {
    fn table_start(table: &'a mut Table) -> Self {
        let page_num = table.root_page_num;
        let root_node = table.pager.get_page(page_num as usize).unwrap();
        let num_cells = leaf_node_num_cells(root_node);
        let end_of_table = num_cells == 0;

        Cursor {
            table,
            page_num: page_num as usize,
            cell_num: 0,
            end_of_table,
        }
    }

    fn table_end(table: &'a mut Table) -> Self {
        let page_num = table.root_page_num;
        let root_node = table.pager.get_page(table.root_page_num as usize).unwrap();
        let cell_num = leaf_node_num_cells(root_node);

        Cursor {
            table,
            page_num: page_num as usize,
            cell_num: cell_num as usize,
            end_of_table: true,
        }
    }

    fn advance(&mut self) {
        let page_num = self.page_num;
        let node = self.table.pager.get_page(page_num).unwrap();

        self.cell_num += 1;
        if self.cell_num >= leaf_node_num_cells(node) as usize {
            self.end_of_table = true;
        }
    }

    fn value(&mut self) -> &mut [u8] {
        let page_num = self.page_num;
        let page = self.table.pager.get_page(page_num).unwrap();

        leaf_node_value(page, self.cell_num as u32)
    }
}

#[derive(Debug)]
enum ExecuteError {
    TableFull,
}

fn execute_insert(statement: &Statement, table: &mut Table) -> Result<(), ExecuteError> {
    let node = table.pager.get_page(table.root_page_num as usize).unwrap();

    if leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS as u32 {
        return Err(ExecuteError::TableFull);
    }

    let row_to_insert = statement.row_to_insert.as_ref().unwrap();
    let mut cursor = Cursor::table_end(table);
    leaf_node_insert(&mut cursor, row_to_insert.id, &row_to_insert);

    Ok(())
}

#[allow(unused_variables)]
fn execute_select(statement: &Statement, table: &mut Table) -> Result<(), ExecuteError> {
    let mut cursor = Cursor::table_start(table);

    while !cursor.end_of_table {
        let row = Row::deserialize(cursor.value());
        row.print();
        cursor.advance();
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

