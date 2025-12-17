import sys
import struct
import os
from enum import Enum, auto

# --- Constants & Configuration ---
COLUMN_USERNAME_SIZE = 32
COLUMN_EMAIL_SIZE = 255

ROW_STRUCT_FORMAT = f'<I{COLUMN_USERNAME_SIZE}s{COLUMN_EMAIL_SIZE}s'
ROW_SIZE = struct.calcsize(ROW_STRUCT_FORMAT)

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100
ROWS_PER_PAGE = PAGE_SIZE // ROW_SIZE
TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

# --- Enums ---
class MetaCommandResult(Enum):
    SUCCESS = auto()
    UNRECOGNIZED_COMMAND = auto()

class PrepareResult(Enum):
    SUCCESS = auto()
    NEGATIVE_ID = auto()
    STRING_TOO_LONG = auto()
    SYNTAX_ERROR = auto()
    UNRECOGNIZED_STATEMENT = auto()

class ExecuteResult(Enum):
    SUCCESS = auto()
    TABLE_FULL = auto()

class StatementType(Enum):
    INSERT = auto()
    SELECT = auto()

# --- Data Structures ---

class Row:
    def __init__(self, id_val=0, username="", email=""):
        self.id = id_val
        self.username = username
        self.email = email

    def __repr__(self):
        return f"({self.id}, {self.username}, {self.email})"

class Statement:
    def __init__(self):
        self.type = None
        self.row_to_insert = None

class Pager:
    def __init__(self, filename):
        self.filename = filename
        
        # Equivalent to O_RDWR | O_CREAT
        # We ensure file exists before opening in r+b mode
        if not os.path.exists(filename):
            with open(filename, 'wb') as f:
                pass 
        
        self.file = open(filename, 'r+b')
        self.file_length = os.path.getsize(filename)
        
        # In C: void* pages[TABLE_MAX_PAGES];
        # In Python: A dict acts as a sparse array for caching pages
        self.pages = {} 

    def get_page(self, page_num):
        if page_num > TABLE_MAX_PAGES:
            print(f"Tried to fetch page number out of bounds. {page_num} > {TABLE_MAX_PAGES}")
            sys.exit(1)

        # Check Cache
        if page_num in self.pages:
            return self.pages[page_num]

        # Cache Miss: Allocate memory and load from file
        # In C, malloc gives random memory. In Python, we create a clean bytearray.
        page = bytearray(PAGE_SIZE)
        
        num_pages = self.file_length // PAGE_SIZE
        # We might save a partial page at the end of the file
        if self.file_length % PAGE_SIZE:
            num_pages += 1

        if page_num <= num_pages:
            self.file.seek(page_num * PAGE_SIZE)
            bytes_read = self.file.read(PAGE_SIZE)
            # Copy read bytes into our fixed-size buffer
            page[0:len(bytes_read)] = bytes_read

        self.pages[page_num] = page
        return page

    def flush(self, page_num, size):
        if page_num not in self.pages:
            print("Tried to flush null page")
            sys.exit(1)

        self.file.seek(page_num * PAGE_SIZE)
        # Write only the specified amount of bytes (used for partial pages)
        self.file.write(self.pages[page_num][:size])


class Table:
    def __init__(self, filename):
        self.pager = Pager(filename)
        self.num_rows = self.pager.file_length // ROW_SIZE

    def close(self):
        pager = self.pager
        num_full_pages = self.num_rows // ROWS_PER_PAGE

        # Write all full pages to disk
        for i in range(num_full_pages):
            if i in pager.pages:
                pager.flush(i, PAGE_SIZE)
                # In Python GC handles free, but we remove reference to mimic C
                del pager.pages[i]

        # Write partial page to end of file
        num_additional_rows = self.num_rows % ROWS_PER_PAGE
        if num_additional_rows > 0:
            page_num = num_full_pages
            if page_num in pager.pages:
                pager.flush(page_num, num_additional_rows * ROW_SIZE)
                del pager.pages[page_num]

        pager.file.close()

# --- Serialization ---

def serialize_row(row):
    username_bytes = row.username.encode('ascii')
    email_bytes = row.email.encode('ascii')
    return struct.pack(ROW_STRUCT_FORMAT, row.id, username_bytes, email_bytes)

def deserialize_row(data):
    unpacked = struct.unpack(ROW_STRUCT_FORMAT, data)
    row = Row(
        id_val=unpacked[0],
        username=unpacked[1].decode('ascii').rstrip('\x00'),
        email=unpacked[2].decode('ascii').rstrip('\x00')
    )
    return row

def row_slot(table, row_num):
    page_num = row_num // ROWS_PER_PAGE
    
    # Replaced manual table.pages access with table.pager.get_page()
    page = table.pager.get_page(page_num)
    
    row_offset = row_num % ROWS_PER_PAGE
    byte_offset = row_offset * ROW_SIZE
    return page, byte_offset

# --- Execution ---

def execute_insert(statement, table):
    if table.num_rows >= TABLE_MAX_ROWS:
        return ExecuteResult.TABLE_FULL
    
    row = statement.row_to_insert
    page, offset = row_slot(table, table.num_rows)
    page[offset : offset + ROW_SIZE] = serialize_row(row)
    table.num_rows += 1
    return ExecuteResult.SUCCESS

def execute_select(statement, table):
    for i in range(table.num_rows):
        page, offset = row_slot(table, i)
        row_bytes = page[offset : offset + ROW_SIZE]
        print(deserialize_row(row_bytes))
    return ExecuteResult.SUCCESS

def execute_statement(statement, table):
    if statement.type == StatementType.INSERT:
        return execute_insert(statement, table)
    elif statement.type == StatementType.SELECT:
        return execute_select(statement, table)

# --- Parsing ---

def do_meta_command(user_input, table):
    if user_input == ".exit":
        table.close()
        sys.exit(0)
    else:
        return MetaCommandResult.UNRECOGNIZED_COMMAND

def prepare_insert(user_input, statement):
    statement.type = StatementType.INSERT
    parts = user_input.split()
    if len(parts) != 4:
        return PrepareResult.SYNTAX_ERROR
    try:
        r_id = int(parts[1])
        r_username = parts[2]
        r_email = parts[3]
    except ValueError:
        return PrepareResult.SYNTAX_ERROR
    if r_id < 0: return PrepareResult.NEGATIVE_ID
    if len(r_username) > COLUMN_USERNAME_SIZE: return PrepareResult.STRING_TOO_LONG
    if len(r_email) > COLUMN_EMAIL_SIZE: return PrepareResult.STRING_TOO_LONG

    statement.row_to_insert = Row(r_id, r_username, r_email)
    return PrepareResult.SUCCESS

def prepare_statement(user_input, statement):
    if user_input.startswith("insert"):
        return prepare_insert(user_input, statement)
    if user_input.startswith("select"):
        statement.type = StatementType.SELECT
        return PrepareResult.SUCCESS
    return PrepareResult.UNRECOGNIZED_STATEMENT

# --- Main ---

def main():
    if len(sys.argv) < 2:
        print("Must supply a database filename.")
        sys.exit(1)
    
    filename = sys.argv[1]
    table = Table(filename)
    
    while True:
        try:
            user_input = input("db > ")
        except EOFError:
            sys.exit(0)

        if user_input.startswith("."):
            meta_result = do_meta_command(user_input, table)
            if meta_result == MetaCommandResult.SUCCESS:
                continue
            elif meta_result == MetaCommandResult.UNRECOGNIZED_COMMAND:
                print(f"Unrecognized command '{user_input}'")
                continue
        
        statement = Statement()
        prepare_result = prepare_statement(user_input, statement)

        if prepare_result == PrepareResult.SUCCESS:
            result = execute_statement(statement, table)
            if result == ExecuteResult.SUCCESS:
                print("Executed.")
            elif result == ExecuteResult.TABLE_FULL:
                print("Error: Table full.")
        elif prepare_result == PrepareResult.SYNTAX_ERROR:
            print("Syntax error. Could not parse statement.")
        elif prepare_result == PrepareResult.NEGATIVE_ID:
            print("ID must be positive.")
        elif prepare_result == PrepareResult.STRING_TOO_LONG:
            print("String is too long.")
        elif prepare_result == PrepareResult.UNRECOGNIZED_STATEMENT:
            print(f"Unrecognized keyword at start of '{user_input}'.")

if __name__ == "__main__":
    main()