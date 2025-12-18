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

# --- Node Header Layout (Common) ---
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

# --- Internal Node Header Layout ---
INTERNAL_NODE_NUM_KEYS_SIZE = 4
INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE
INTERNAL_NODE_RIGHT_CHILD_SIZE = 4
INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE

# --- Internal Node Body Layout ---
INTERNAL_NODE_KEY_SIZE = 4
INTERNAL_NODE_CHILD_SIZE = 4
INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE

# --- Splitting Constants ---
LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) // 2
LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT

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
    DUPLICATE_KEY = auto()

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
            with open(filename, 'wb') as f: pass 
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
    
    def get_unused_page_num(self):
        return self.num_pages

class Table:
    def __init__(self, filename):
        self.pager = Pager(filename)
        self.root_page_num = 0
        if self.pager.num_pages == 0:
            # Initialize root as leaf
            root_node = self.pager.get_page(0)
            initialize_leaf_node(root_node)
            set_node_root(root_node, True)

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

# --- Common Node Functions ---

def get_node_type(node):
    value = struct.unpack_from('B', node, NODE_TYPE_OFFSET)[0]
    return NodeType(value)

def set_node_type(node, type_enum):
    struct.pack_into('B', node, NODE_TYPE_OFFSET, type_enum.value)

def is_node_root(node):
    value = struct.unpack_from('B', node, IS_ROOT_OFFSET)[0]
    return value != 0

def set_node_root(node, is_root):
    value = 1 if is_root else 0
    struct.pack_into('B', node, IS_ROOT_OFFSET, value)

def get_node_max_key(node):
    node_type = get_node_type(node)
    if node_type == NodeType.INTERNAL:
        return internal_node_key(node, internal_node_num_keys(node) - 1)
    elif node_type == NodeType.LEAF:
        return leaf_node_key(node, leaf_node_num_cells(node) - 1)

# --- Leaf Node Functions ---

def initialize_leaf_node(node):
    set_node_type(node, NodeType.LEAF)
    set_node_root(node, False)
    struct.pack_into('<I', node, LEAF_NODE_NUM_CELLS_OFFSET, 0)

def leaf_node_num_cells(node):
    return struct.unpack_from('<I', node, LEAF_NODE_NUM_CELLS_OFFSET)[0]

def set_leaf_node_num_cells(node, num):
    struct.pack_into('<I', node, LEAF_NODE_NUM_CELLS_OFFSET, num)

def leaf_node_cell(cell_num):
    return LEAF_NODE_HEADER_SIZE + (cell_num * LEAF_NODE_CELL_SIZE)

def leaf_node_key(node, cell_num):
    offset = leaf_node_cell(cell_num)
    return struct.unpack_from('<I', node, offset)[0]

def leaf_node_value(node, cell_num):
    offset = leaf_node_cell(cell_num) + LEAF_NODE_KEY_SIZE
    return node[offset : offset + LEAF_NODE_VALUE_SIZE]

# --- Internal Node Functions ---

def initialize_internal_node(node):
    set_node_type(node, NodeType.INTERNAL)
    set_node_root(node, False)
    struct.pack_into('<I', node, INTERNAL_NODE_NUM_KEYS_OFFSET, 0)

def internal_node_num_keys(node):
    return struct.unpack_from('<I', node, INTERNAL_NODE_NUM_KEYS_OFFSET)[0]

def set_internal_node_num_keys(node, num):
    struct.pack_into('<I', node, INTERNAL_NODE_NUM_KEYS_OFFSET, num)

def internal_node_right_child(node):
    return struct.unpack_from('<I', node, INTERNAL_NODE_RIGHT_CHILD_OFFSET)[0]

def set_internal_node_right_child(node, child_page_num):
    struct.pack_into('<I', node, INTERNAL_NODE_RIGHT_CHILD_OFFSET, child_page_num)

def internal_node_cell(cell_num):
    return INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE

def internal_node_child(node, child_num):
    num_keys = internal_node_num_keys(node)
    if child_num > num_keys:
        print(f"Tried to access child_num {child_num} > num_keys {num_keys}")
        sys.exit(1)
    elif child_num == num_keys:
        return internal_node_right_child(node)
    else:
        offset = internal_node_cell(child_num)
        return struct.unpack_from('<I', node, offset)[0]

def set_internal_node_child(node, child_num, child_page_num):
    offset = internal_node_cell(child_num)
    struct.pack_into('<I', node, offset, child_page_num)

def internal_node_key(node, key_num):
    offset = internal_node_cell(key_num) + INTERNAL_NODE_CHILD_SIZE
    return struct.unpack_from('<I', node, offset)[0]

def set_internal_node_key(node, key_num, key):
    offset = internal_node_cell(key_num) + INTERNAL_NODE_CHILD_SIZE
    struct.pack_into('<I', node, offset, key)

# --- Tree Logic (Split & Insert) ---

def create_new_root(table, right_child_page_num):
    root = table.pager.get_page(table.root_page_num)
    right_child = table.pager.get_page(right_child_page_num)
    left_child_page_num = table.pager.get_unused_page_num()
    left_child = table.pager.get_page(left_child_page_num)
    
    # Left child has data copied from old root
    # Note: In Python, slice assignment `[:]` copies content
    left_child[:] = root[:]
    set_node_root(left_child, False)
    
    # Root node is a new internal node with one key and two children
    initialize_internal_node(root)
    set_node_root(root, True)
    set_internal_node_num_keys(root, 1)
    
    set_internal_node_child(root, 0, left_child_page_num)
    left_child_max_key = get_node_max_key(left_child)
    set_internal_node_key(root, 0, left_child_max_key)
    set_internal_node_right_child(root, right_child_page_num)

def leaf_node_split_and_insert(cursor, key, value):
    old_node = cursor.table.pager.get_page(cursor.page_num)
    new_page_num = cursor.table.pager.get_unused_page_num()
    new_node = cursor.table.pager.get_page(new_page_num)
    initialize_leaf_node(new_node)
    
    # Move cells to new node based on split count
    # Iterate backwards to handle shifting correctly
    for i in range(LEAF_NODE_MAX_CELLS, -1, -1):
        if i >= LEAF_NODE_LEFT_SPLIT_COUNT:
            destination_node = new_node
        else:
            destination_node = old_node
            
        index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT
        dest_offset = leaf_node_cell(index_within_node)
        
        if i == cursor.cell_num:
            # Serialize the new row here
            struct.pack_into('<I', destination_node, dest_offset, key)
            serialize_row(value, destination_node, dest_offset + LEAF_NODE_KEY_SIZE)
        elif i > cursor.cell_num:
            # Copy from old_node (i - 1)
            src_offset = leaf_node_cell(i - 1)
            # Python slice copy
            chunk = old_node[src_offset : src_offset + LEAF_NODE_CELL_SIZE]
            destination_node[dest_offset : dest_offset + LEAF_NODE_CELL_SIZE] = chunk
        else:
            # Copy from old_node (i)
            src_offset = leaf_node_cell(i)
            chunk = old_node[src_offset : src_offset + LEAF_NODE_CELL_SIZE]
            destination_node[dest_offset : dest_offset + LEAF_NODE_CELL_SIZE] = chunk

    # Update cell counts
    set_leaf_node_num_cells(old_node, LEAF_NODE_LEFT_SPLIT_COUNT)
    set_leaf_node_num_cells(new_node, LEAF_NODE_RIGHT_SPLIT_COUNT)
    
    if is_node_root(old_node):
        create_new_root(cursor.table, new_page_num)
    else:
        print("Need to implement updating parent after split")
        sys.exit(1)

def leaf_node_insert(cursor, key, value):
    node = cursor.table.pager.get_page(cursor.page_num)
    num_cells = leaf_node_num_cells(node)
    
    if num_cells >= LEAF_NODE_MAX_CELLS:
        leaf_node_split_and_insert(cursor, key, value)
        return
        
    if cursor.cell_num < num_cells:
        # Make room for new cell (Shift bytes to the right)
        start_offset = leaf_node_cell(cursor.cell_num)
        end_offset = leaf_node_cell(num_cells)
        bytes_to_move = node[start_offset:end_offset]
        
        dest_offset = start_offset + LEAF_NODE_CELL_SIZE
        node[dest_offset : dest_offset + len(bytes_to_move)] = bytes_to_move
        
    set_leaf_node_num_cells(node, num_cells + 1)
    
    offset = leaf_node_cell(cursor.cell_num)
    struct.pack_into('<I', node, offset, key)
    serialize_row(value, node, offset + LEAF_NODE_KEY_SIZE)

# --- Search Logic ---

def leaf_node_find(table, page_num, key):
    node = table.pager.get_page(page_num)
    num_cells = leaf_node_num_cells(node)
    cursor = Cursor(table, page_num, 0)

    min_index = 0
    one_past_max_index = num_cells
    
    while one_past_max_index != min_index:
        index = (min_index + one_past_max_index) // 2
        key_at_index = leaf_node_key(node, index)
        if key == key_at_index:
            cursor.cell_num = index
            return cursor
        if key < key_at_index:
            one_past_max_index = index
        else:
            min_index = index + 1
            
    cursor.cell_num = min_index
    return cursor

def table_find(table, key):
    root_page_num = table.root_page_num
    root_node = table.pager.get_page(root_page_num)

    if get_node_type(root_node) == NodeType.LEAF:
        return leaf_node_find(table, root_page_num, key)
    else:
        print("Need to implement searching an internal node")
        sys.exit(1)

# --- Serialization ---

def serialize_row(row, page, offset):
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

# --- Main & Execution ---

def print_tree(pager, page_num, indentation_level):
    node = pager.get_page(page_num)
    node_type = get_node_type(node)
    
    if node_type == NodeType.LEAF:
        num_keys = leaf_node_num_cells(node)
        print("  " * indentation_level + f"- leaf (size {num_keys})")
        for i in range(num_keys):
            key = leaf_node_key(node, i)
            print("  " * (indentation_level + 1) + f"- {key}")
            
    elif node_type == NodeType.INTERNAL:
        num_keys = internal_node_num_keys(node)
        print("  " * indentation_level + f"- internal (size {num_keys})")
        for i in range(num_keys):
            child = internal_node_child(node, i)
            print_tree(pager, child, indentation_level + 1)
            
            key = internal_node_key(node, i)
            print("  " * (indentation_level + 1) + f"- key {key}")
            
        child = internal_node_right_child(node)
        print_tree(pager, child, indentation_level + 1)

def do_meta_command(user_input, table):
    if user_input == ".exit":
        table.close()
        sys.exit(0)
    elif user_input == ".btree":
        print("Tree:")
        print_tree(table.pager, 0, 0)
        return MetaCommandResult.SUCCESS
    else:
        return MetaCommandResult.UNRECOGNIZED_COMMAND

def prepare_insert(user_input, statement):
    statement.type = StatementType.INSERT
    parts = user_input.split()
    if len(parts) != 4: return PrepareResult.SYNTAX_ERROR
    try:
        r_id = int(parts[1])
        r_username = parts[2]
        r_email = parts[3]
    except ValueError: return PrepareResult.SYNTAX_ERROR
    if r_id < 0: return PrepareResult.NEGATIVE_ID
    if len(r_username) > COLUMN_USERNAME_SIZE: return PrepareResult.STRING_TOO_LONG
    if len(r_email) > COLUMN_EMAIL_SIZE: return PrepareResult.STRING_TOO_LONG

    statement.row_to_insert = Row(r_id, r_username, r_email)
    return PrepareResult.SUCCESS

def prepare_statement(user_input, statement):
    if user_input.startswith("insert"): return prepare_insert(user_input, statement)
    if user_input.startswith("select"):
        statement.type = StatementType.SELECT
        return PrepareResult.SUCCESS
    return PrepareResult.UNRECOGNIZED_STATEMENT

def execute_insert(statement, table):
    node = table.pager.get_page(table.root_page_num)
    num_cells = leaf_node_num_cells(node)
    # Replaced check for FULL with split logic inside leaf_node_insert
    
    row = statement.row_to_insert
    key = row.id
    cursor = table_find(table, key)

    if cursor.cell_num < num_cells:
        key_at_index = leaf_node_key(node, cursor.cell_num)
        if key_at_index == key:
            return ExecuteResult.DUPLICATE_KEY

    leaf_node_insert(cursor, key, row)
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

def table_start(table):
    cursor = Cursor(table, table.root_page_num, 0)
    # Temporary: table_start logic for just single node or root node
    # Since we don't have full tree traversal in iterator yet, we just start at root.
    # NOTE: The tutorial hasn't updated table_start to recurse down to the leftmost leaf yet.
    # It will only work for single-node trees until the next chapter.
    node = table.pager.get_page(table.root_page_num)
    num_cells = leaf_node_num_cells(node)
    cursor.end_of_table = (num_cells == 0)
    return cursor

def cursor_value(cursor):
    node = cursor.table.pager.get_page(cursor.page_num)
    return leaf_node_value(node, cursor.cell_num)

def cursor_advance(cursor):
    node = cursor.table.pager.get_page(cursor.page_num)
    cursor.cell_num += 1
    if cursor.cell_num >= leaf_node_num_cells(node):
        cursor.end_of_table = True

def main():
    if len(sys.argv) < 2:
        print("Must supply a database filename.")
        sys.exit(1)
    filename = sys.argv[1]
    table = Table(filename)
    
    while True:
        try:
            user_input = input("db > ")
        except EOFError: sys.exit(0)
        if user_input.startswith("."):
            meta_result = do_meta_command(user_input, table)
            if meta_result == MetaCommandResult.SUCCESS: continue
            elif meta_result == MetaCommandResult.UNRECOGNIZED_COMMAND:
                print(f"Unrecognized command '{user_input}'")
                continue
        statement = Statement()
        prepare_result = prepare_statement(user_input, statement)
        if prepare_result == PrepareResult.SUCCESS:
            result = execute_statement(statement, table)
            if result == ExecuteResult.SUCCESS: print("Executed.")
            elif result == ExecuteResult.DUPLICATE_KEY: print("Error: Duplicate key.")
            elif result == ExecuteResult.TABLE_FULL: print("Error: Table full.")
        elif prepare_result == PrepareResult.SYNTAX_ERROR: print("Syntax error. Could not parse statement.")
        elif prepare_result == PrepareResult.NEGATIVE_ID: print("ID must be positive.")
        elif prepare_result == PrepareResult.STRING_TOO_LONG: print("String is too long.")
        elif prepare_result == PrepareResult.UNRECOGNIZED_STATEMENT: print(f"Unrecognized keyword at start of '{user_input}'.")

if __name__ == "__main__":
    main()