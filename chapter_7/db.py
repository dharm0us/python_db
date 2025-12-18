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

# --- Node Header Layout ---
NODE_TYPE_SIZE = 1
NODE_TYPE_OFFSET = 0
IS_ROOT_SIZE = 1
IS_ROOT_OFFSET = NODE_TYPE_SIZE
PARENT_POINTER_SIZE = 4
PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE
COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

# --- Leaf Node Header Layout ---
LEAF_NODE_NUM_CELLS_SIZE = 4
LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE

# --- Leaf Node Body Layout ---
LEAF_NODE_KEY_SIZE = 4
LEAF_NODE_KEY_OFFSET = 0
LEAF_NODE_VALUE_SIZE = ROW_SIZE
LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS // LEAF_NODE_CELL_SIZE

# --- Enums ---
class NodeType(Enum):
    INTERNAL = 0
    LEAF = 1

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
        if not os.path.exists(filename):
            with open(filename, 'wb') as f:
                pass 
        
        self.file = open(filename, 'r+b')
        self.file_length = os.path.getsize(filename)
        self.num_pages = self.file_length // PAGE_SIZE
        
        if self.file_length % PAGE_SIZE != 0:
            print("Db file is not a whole number of pages. Corrupt file.")
            sys.exit(1)
            
        self.pages = {} 

    def get_page(self, page_num):
        if page_num > TABLE_MAX_PAGES:
            print(f"Tried to fetch page number out of bounds. {page_num} > {TABLE_MAX_PAGES}")
            sys.exit(1)

        if page_num in self.pages:
            return self.pages[page_num]

        page = bytearray(PAGE_SIZE)
        
        if page_num < self.num_pages:
            self.file.seek(page_num * PAGE_SIZE)
            bytes_read = self.file.read(PAGE_SIZE)
            page[0:len(bytes_read)] = bytes_read

        self.pages[page_num] = page
        
        if page_num >= self.num_pages:
             self.num_pages = page_num + 1
             
        return page

    def flush(self, page_num):
        if page_num not in self.pages:
            print("Tried to flush null page")
            sys.exit(1)
        self.file.seek(page_num * PAGE_SIZE)
        self.file.write(self.pages[page_num])

class Table:
    def __init__(self, filename):
        self.pager = Pager(filename)
        self.root_page_num = 0
        
        if self.pager.num_pages == 0:
            # New DB: Initialize page 0 as a leaf node
            root_node = self.pager.get_page(0)
            initialize_leaf_node(root_node)

    def close(self):
        pager = self.pager
        for i in range(pager.num_pages):
            if i in pager.pages:
                pager.flush(i)
                del pager.pages[i]
        pager.file.close()

class Cursor:
    def __init__(self, table, page_num, cell_num, end_of_table=False):
        self.table = table
        self.page_num = page_num
        self.cell_num = cell_num
        self.end_of_table = end_of_table

# --- B-Tree Leaf Node Functions ---

def initialize_leaf_node(node):
    """Sets the node type and num_cells to 0"""
    # Set Node Type to LEAF (1)
    struct.pack_into('B', node, NODE_TYPE_OFFSET, NodeType.LEAF.value)
    # Set Is Root to 0 (false) - simplified for now
    struct.pack_into('B', node, IS_ROOT_OFFSET, 0) 
    # Set Num Cells to 0
    struct.pack_into('<I', node, LEAF_NODE_NUM_CELLS_OFFSET, 0)

def leaf_node_num_cells(node):
    """Reads the number of cells from the header"""
    # Returns a tuple, so we get [0]
    return struct.unpack_from('<I', node, LEAF_NODE_NUM_CELLS_OFFSET)[0]

def leaf_node_cell(node, cell_num):
    """Calculates offset for a specific cell"""
    return LEAF_NODE_HEADER_SIZE + (cell_num * LEAF_NODE_CELL_SIZE)

def leaf_node_key(node, cell_num):
    """Reads the Key (ID) from a cell"""
    offset = leaf_node_cell(node, cell_num)
    return struct.unpack_from('<I', node, offset)[0]

def leaf_node_value(node, cell_num):
    """Reads the Value (Row bytes) from a cell"""
    offset = leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE
    return node[offset : offset + LEAF_NODE_VALUE_SIZE]

def leaf_node_insert(cursor, key, value):
    node = cursor.table.pager.get_page(cursor.page_num)
    num_cells = leaf_node_num_cells(node)
    
    if num_cells >= LEAF_NODE_MAX_CELLS:
        # Node full
        print("Need to implement splitting a leaf node.")
        sys.exit(1)
        
    if cursor.cell_num < num_cells:
        # Make room for new cell (Shift bytes to the right)
        # 1. Calculate bytes to move
        start_offset = leaf_node_cell(node, cursor.cell_num)
        end_offset = leaf_node_cell(node, num_cells) # End of used space
        bytes_to_move = node[start_offset:end_offset]
        
        # 2. Write them to new position (one cell size to the right)
        dest_offset = start_offset + LEAF_NODE_CELL_SIZE
        node[dest_offset : dest_offset + len(bytes_to_move)] = bytes_to_move
        
    # Update Num Cells
    struct.pack_into('<I', node, LEAF_NODE_NUM_CELLS_OFFSET, num_cells + 1)
    
    # Write Key
    offset = leaf_node_cell(node, cursor.cell_num)
    struct.pack_into('<I', node, offset, key)
    
    # Write Value
    serialize_row(value, node, offset + LEAF_NODE_KEY_SIZE)

# --- Cursor Logic ---

def table_start(table):
    cursor = Cursor(table, table.root_page_num, 0)
    root_node = table.pager.get_page(table.root_page_num)
    num_cells = leaf_node_num_cells(root_node)
    cursor.end_of_table = (num_cells == 0)
    return cursor

def table_end(table):
    root_node = table.pager.get_page(table.root_page_num)
    num_cells = leaf_node_num_cells(root_node)
    return Cursor(table, table.root_page_num, num_cells, True)

def cursor_value(cursor):
    node = cursor.table.pager.get_page(cursor.page_num)
    return leaf_node_value(node, cursor.cell_num)

def cursor_advance(cursor):
    node = cursor.table.pager.get_page(cursor.page_num)
    cursor.cell_num += 1
    if cursor.cell_num >= leaf_node_num_cells(node):
        cursor.end_of_table = True

# --- Serialization ---

def serialize_row(row, page, offset):
    """Writes row directly into page memory at offset"""
    username_bytes = row.username.encode('ascii')
    email_bytes = row.email.encode('ascii')
    struct.pack_into(ROW_STRUCT_FORMAT, page, offset, row.id, username_bytes, email_bytes)

def deserialize_row(data):
    unpacked = struct.unpack(ROW_STRUCT_FORMAT, data)
    row = Row(
        id_val=unpacked[0],
        username=unpacked[1].decode('ascii').rstrip('\x00'),
        email=unpacked[2].decode('ascii').rstrip('\x00')
    )
    return row

# --- Execution ---

def execute_insert(statement, table):
    node = table.pager.get_page(table.root_page_num)
    num_cells = leaf_node_num_cells(node)
    
    if num_cells >= LEAF_NODE_MAX_CELLS:
        return ExecuteResult.TABLE_FULL
    
    row = statement.row_to_insert
    cursor = table_end(table)
    
    leaf_node_insert(cursor, row.id, row)
    
    return ExecuteResult.SUCCESS

def execute_select(statement, table):
    cursor = table_start(table)
    while not cursor.end_of_table:
        row_bytes = cursor_value(cursor)
        print(deserialize_row(row_bytes))
        cursor_advance(cursor)
    return ExecuteResult.SUCCESS

def execute_statement(statement, table):
    if statement.type == StatementType.INSERT:
        return execute_insert(statement, table)
    elif statement.type == StatementType.SELECT:
        return execute_select(statement, table)

# --- Parsing & Helper ---

def print_constants():
    print(f"ROW_SIZE: {ROW_SIZE}")
    print(f"COMMON_NODE_HEADER_SIZE: {COMMON_NODE_HEADER_SIZE}")
    print(f"LEAF_NODE_HEADER_SIZE: {LEAF_NODE_HEADER_SIZE}")
    print(f"LEAF_NODE_CELL_SIZE: {LEAF_NODE_CELL_SIZE}")
    print(f"LEAF_NODE_SPACE_FOR_CELLS: {LEAF_NODE_SPACE_FOR_CELLS}")
    print(f"LEAF_NODE_MAX_CELLS: {LEAF_NODE_MAX_CELLS}")

def print_leaf_node(node):
    num_cells = leaf_node_num_cells(node)
    print(f"leaf (size {num_cells})")
    for i in range(num_cells):
        key = leaf_node_key(node, i)
        print(f"  - {i} : {key}")

def do_meta_command(user_input, table):
    if user_input == ".exit":
        table.close()
        sys.exit(0)
    elif user_input == ".btree":
        print("Tree:")
        print_leaf_node(table.pager.get_page(0))
        return MetaCommandResult.SUCCESS
    elif user_input == ".constants":
        print("Constants:")
        print_constants()
        return MetaCommandResult.SUCCESS
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