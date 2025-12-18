import sys
import struct
import os
from enum import Enum, auto

# --- Constants & Configuration ---
COLUMN_USERNAME_SIZE = 32
COLUMN_EMAIL_SIZE = 255

# Python struct format: Little Endian (<), Unsigned Int (I), Strings (s)
ROW_STRUCT_FORMAT = f'<I{COLUMN_USERNAME_SIZE}s{COLUMN_EMAIL_SIZE}s'
ROW_SIZE = struct.calcsize(ROW_STRUCT_FORMAT)  # 291 bytes

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 400
INVALID_PAGE_NUM = 0xFFFFFFFF  # Equivalent to UINT32_MAX

# --- Node Header Layout (Common) ---
NODE_TYPE_SIZE = 1
NODE_TYPE_OFFSET = 0
IS_ROOT_SIZE = 1
IS_ROOT_OFFSET = NODE_TYPE_SIZE
PARENT_POINTER_SIZE = 4
PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE
COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

# --- Internal Node Layout ---
INTERNAL_NODE_NUM_KEYS_SIZE = 4
INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE
INTERNAL_NODE_RIGHT_CHILD_SIZE = 4
INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
INTERNAL_NODE_HEADER_SIZE = (COMMON_NODE_HEADER_SIZE + 
                             INTERNAL_NODE_NUM_KEYS_SIZE + 
                             INTERNAL_NODE_RIGHT_CHILD_SIZE)

INTERNAL_NODE_KEY_SIZE = 4
INTERNAL_NODE_CHILD_SIZE = 4
INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE
INTERNAL_NODE_MAX_KEYS = 3  # Kept small for testing, as per C code

# --- Leaf Node Layout ---
LEAF_NODE_NUM_CELLS_SIZE = 4
LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
LEAF_NODE_NEXT_LEAF_SIZE = 4
LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE
LEAF_NODE_HEADER_SIZE = (COMMON_NODE_HEADER_SIZE + 
                         LEAF_NODE_NUM_CELLS_SIZE + 
                         LEAF_NODE_NEXT_LEAF_SIZE)

LEAF_NODE_KEY_SIZE = 4
LEAF_NODE_KEY_OFFSET = 0
LEAF_NODE_VALUE_SIZE = ROW_SIZE
LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS // LEAF_NODE_CELL_SIZE

# --- Split Counts ---
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

# --- Data Objects ---

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

        # Cache miss. Allocate memory
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
            # New DB: Initialize page 0 as root leaf
            root_node = self.pager.get_page(0)
            initialize_leaf_node(root_node)
            set_node_root(root_node, True)

    def close(self):
        for i in range(self.pager.num_pages):
            if i in self.pager.pages:
                self.pager.flush(i)
                del self.pager.pages[i]
        self.pager.file.close()

class Cursor:
    def __init__(self, table, page_num, cell_num, end_of_table=False):
        self.table = table
        self.page_num = page_num
        self.cell_num = cell_num
        self.end_of_table = end_of_table

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

# --- Common Node Accessors ---

def get_node_type(node):
    return NodeType(struct.unpack_from('B', node, NODE_TYPE_OFFSET)[0])

def set_node_type(node, node_type):
    struct.pack_into('B', node, NODE_TYPE_OFFSET, node_type.value)

def is_node_root(node):
    return struct.unpack_from('B', node, IS_ROOT_OFFSET)[0] != 0

def set_node_root(node, is_root):
    val = 1 if is_root else 0
    struct.pack_into('B', node, IS_ROOT_OFFSET, val)

def node_parent(node):
    # Returns the value, not the pointer. In C: return node + PARENT_POINTER_OFFSET
    # But since we use bytearrays, we usually need getters/setters.
    return struct.unpack_from('<I', node, PARENT_POINTER_OFFSET)[0]

def set_node_parent(node, parent_page_num):
    struct.pack_into('<I', node, PARENT_POINTER_OFFSET, parent_page_num)

def get_node_max_key(pager, node):
    if get_node_type(node) == NodeType.LEAF:
        return leaf_node_key(node, leaf_node_num_cells(node) - 1)
    
    right_child_page = internal_node_right_child(node)
    right_child_node = pager.get_page(right_child_page)
    return get_node_max_key(pager, right_child_node)

# --- Leaf Node Accessors ---

def initialize_leaf_node(node):
    set_node_type(node, NodeType.LEAF)
    set_node_root(node, False)
    set_leaf_node_num_cells(node, 0)
    set_leaf_node_next_leaf(node, 0) # 0 represents no sibling

def leaf_node_num_cells(node):
    return struct.unpack_from('<I', node, LEAF_NODE_NUM_CELLS_OFFSET)[0]

def set_leaf_node_num_cells(node, num):
    struct.pack_into('<I', node, LEAF_NODE_NUM_CELLS_OFFSET, num)

def leaf_node_next_leaf(node):
    return struct.unpack_from('<I', node, LEAF_NODE_NEXT_LEAF_OFFSET)[0]

def set_leaf_node_next_leaf(node, next_leaf):
    struct.pack_into('<I', node, LEAF_NODE_NEXT_LEAF_OFFSET, next_leaf)

def leaf_node_cell_offset(cell_num):
    return LEAF_NODE_HEADER_SIZE + (cell_num * LEAF_NODE_CELL_SIZE)

def leaf_node_key(node, cell_num):
    offset = leaf_node_cell_offset(cell_num)
    return struct.unpack_from('<I', node, offset)[0]

def set_leaf_node_key(node, cell_num, key):
    offset = leaf_node_cell_offset(cell_num)
    struct.pack_into('<I', node, offset, key)

def leaf_node_value_offset(cell_num):
    return leaf_node_cell_offset(cell_num) + LEAF_NODE_KEY_SIZE

# --- Internal Node Accessors ---

def initialize_internal_node(node):
    set_node_type(node, NodeType.INTERNAL)
    set_node_root(node, False)
    set_internal_node_num_keys(node, 0)
    set_internal_node_right_child(node, INVALID_PAGE_NUM)

def internal_node_num_keys(node):
    return struct.unpack_from('<I', node, INTERNAL_NODE_NUM_KEYS_OFFSET)[0]

def set_internal_node_num_keys(node, num):
    struct.pack_into('<I', node, INTERNAL_NODE_NUM_KEYS_OFFSET, num)

def internal_node_right_child(node):
    return struct.unpack_from('<I', node, INTERNAL_NODE_RIGHT_CHILD_OFFSET)[0]

def set_internal_node_right_child(node, right_child):
    struct.pack_into('<I', node, INTERNAL_NODE_RIGHT_CHILD_OFFSET, right_child)

def internal_node_cell_offset(cell_num):
    return INTERNAL_NODE_HEADER_SIZE + (cell_num * INTERNAL_NODE_CELL_SIZE)

def internal_node_child(node, child_num):
    num_keys = internal_node_num_keys(node)
    if child_num > num_keys:
        print(f"Tried to access child_num {child_num} > num_keys {num_keys}")
        sys.exit(1)
    elif child_num == num_keys:
        return internal_node_right_child(node)
    else:
        offset = internal_node_cell_offset(child_num)
        return struct.unpack_from('<I', node, offset)[0]

def set_internal_node_child(node, child_num, child_page_num):
    num_keys = internal_node_num_keys(node)
    if child_num == num_keys:
        set_internal_node_right_child(node, child_page_num)
    else:
        offset = internal_node_cell_offset(child_num)
        struct.pack_into('<I', node, offset, child_page_num)

def internal_node_key(node, key_num):
    offset = internal_node_cell_offset(key_num) + INTERNAL_NODE_CHILD_SIZE
    return struct.unpack_from('<I', node, offset)[0]

def set_internal_node_key(node, key_num, key):
    offset = internal_node_cell_offset(key_num) + INTERNAL_NODE_CHILD_SIZE
    struct.pack_into('<I', node, offset, key)

# --- B-Tree Logic ---

def internal_node_find_child(node, key):
    num_keys = internal_node_num_keys(node)
    min_idx = 0
    max_idx = num_keys
    
    while min_idx != max_idx:
        index = (min_idx + max_idx) // 2
        key_to_right = internal_node_key(node, index)
        if key_to_right >= key:
            max_idx = index
        else:
            min_idx = index + 1
    return min_idx

def leaf_node_find(table, page_num, key):
    node = table.pager.get_page(page_num)
    num_cells = leaf_node_num_cells(node)
    cursor = Cursor(table, page_num, 0)
    
    min_idx = 0
    one_past_max = num_cells
    while one_past_max != min_idx:
        index = (min_idx + one_past_max) // 2
        key_at_index = leaf_node_key(node, index)
        if key == key_at_index:
            cursor.cell_num = index
            return cursor
        if key < key_at_index:
            one_past_max = index
        else:
            min_idx = index + 1
            
    cursor.cell_num = min_idx
    return cursor

def internal_node_find(table, page_num, key):
    node = table.pager.get_page(page_num)
    child_index = internal_node_find_child(node, key)
    child_num = internal_node_child(node, child_index)
    
    child = table.pager.get_page(child_num)
    if get_node_type(child) == NodeType.LEAF:
        return leaf_node_find(table, child_num, key)
    else:
        return internal_node_find(table, child_num, key)

def table_find(table, key):
    root_page_num = table.root_page_num
    root_node = table.pager.get_page(root_page_num)
    
    if get_node_type(root_node) == NodeType.LEAF:
        return leaf_node_find(table, root_page_num, key)
    else:
        return internal_node_find(table, root_page_num, key)

def create_new_root(table, right_child_page_num):
    root = table.pager.get_page(table.root_page_num)
    right_child = table.pager.get_page(right_child_page_num)
    left_child_page_num = table.pager.get_unused_page_num()
    left_child = table.pager.get_page(left_child_page_num)
    
    if get_node_type(root) == NodeType.INTERNAL:
        initialize_internal_node(right_child)
        initialize_internal_node(left_child)
        
    # Copy root data to left child
    left_child[:] = root[:]
    set_node_root(left_child, False)
    
    # Update parents for children of the new left child
    if get_node_type(left_child) == NodeType.INTERNAL:
        for i in range(internal_node_num_keys(left_child)):
            child_page_num = internal_node_child(left_child, i)
            child_node = table.pager.get_page(child_page_num)
            set_node_parent(child_node, left_child_page_num)
            
        right_child_of_left = internal_node_right_child(left_child)
        child_node = table.pager.get_page(right_child_of_left)
        set_node_parent(child_node, left_child_page_num)
        
    # Initialize new root
    initialize_internal_node(root)
    set_node_root(root, True)
    set_internal_node_num_keys(root, 1)
    set_internal_node_child(root, 0, left_child_page_num)
    left_child_max = get_node_max_key(table.pager, left_child)
    set_internal_node_key(root, 0, left_child_max)
    set_internal_node_right_child(root, right_child_page_num)
    
    set_node_parent(left_child, table.root_page_num)
    set_node_parent(right_child, table.root_page_num)

def internal_node_insert(table, parent_page_num, child_page_num):
    parent = table.pager.get_page(parent_page_num)
    child = table.pager.get_page(child_page_num)
    child_max_key = get_node_max_key(table.pager, child)
    index = internal_node_find_child(parent, child_max_key)
    
    original_num_keys = internal_node_num_keys(parent)
    
    if original_num_keys >= INTERNAL_NODE_MAX_KEYS:
        internal_node_split_and_insert(table, parent_page_num, child_page_num)
        return

    right_child_page_num = internal_node_right_child(parent)
    
    if right_child_page_num == INVALID_PAGE_NUM:
        set_internal_node_right_child(parent, child_page_num)
        return

    right_child = table.pager.get_page(right_child_page_num)
    set_internal_node_num_keys(parent, original_num_keys + 1)
    
    if child_max_key > get_node_max_key(table.pager, right_child):
        # Replace right child
        set_internal_node_child(parent, original_num_keys, right_child_page_num)
        set_internal_node_key(parent, original_num_keys, get_node_max_key(table.pager, right_child))
        set_internal_node_right_child(parent, child_page_num)
    else:
        # Make room for new cell
        for i in range(original_num_keys, index, -1):
            src_off = internal_node_cell_offset(i - 1)
            dest_off = internal_node_cell_offset(i)
            # Copy cell size (key + child pointer)
            parent[dest_off : dest_off + INTERNAL_NODE_CELL_SIZE] = parent[src_off : src_off + INTERNAL_NODE_CELL_SIZE]
            
        set_internal_node_child(parent, index, child_page_num)
        set_internal_node_key(parent, index, child_max_key)

def update_internal_node_key(node, old_key, new_key):
    old_child_index = internal_node_find_child(node, old_key)
    set_internal_node_key(node, old_child_index, new_key)

def internal_node_split_and_insert(table, parent_page_num, child_page_num):
    old_page_num = parent_page_num
    old_node = table.pager.get_page(parent_page_num)
    old_max = get_node_max_key(table.pager, old_node)
    
    child = table.pager.get_page(child_page_num)
    child_max = get_node_max_key(table.pager, child)
    
    new_page_num = table.pager.get_unused_page_num()
    
    splitting_root = is_node_root(old_node)
    
    if splitting_root:
        create_new_root(table, new_page_num)
        parent = table.pager.get_page(table.root_page_num)
        old_page_num = internal_node_child(parent, 0)
        old_node = table.pager.get_page(old_page_num)
    else:
        parent = table.pager.get_page(node_parent(old_node))
        new_node = table.pager.get_page(new_page_num)
        initialize_internal_node(new_node)
        
    old_num_keys = internal_node_num_keys(old_node)
    cur_page_num = internal_node_right_child(old_node)
    cur = table.pager.get_page(cur_page_num)
    
    # First put right child into new node
    internal_node_insert(table, new_page_num, cur_page_num)
    set_node_parent(cur, new_page_num)
    set_internal_node_right_child(old_node, INVALID_PAGE_NUM)
    
    # Move upper half of keys to new node
    for i in range(INTERNAL_NODE_MAX_KEYS - 1, INTERNAL_NODE_MAX_KEYS // 2, -1):
        cur_page_num = internal_node_child(old_node, i)
        cur = table.pager.get_page(cur_page_num)
        
        internal_node_insert(table, new_page_num, cur_page_num)
        set_node_parent(cur, new_page_num)
        
        old_num_keys -= 1
        set_internal_node_num_keys(old_node, old_num_keys)

    set_internal_node_right_child(old_node, internal_node_child(old_node, old_num_keys - 1))
    old_num_keys -= 1
    set_internal_node_num_keys(old_node, old_num_keys)
    
    max_after_split = get_node_max_key(table.pager, old_node)
    dest_page_num = old_page_num if child_max < max_after_split else new_page_num
    
    internal_node_insert(table, dest_page_num, child_page_num)
    set_node_parent(child, dest_page_num)
    
    update_internal_node_key(parent, old_max, get_node_max_key(table.pager, old_node))
    
    if not splitting_root:
        internal_node_insert(table, node_parent(old_node), new_page_num)
        new_node = table.pager.get_page(new_page_num)
        set_node_parent(new_node, node_parent(old_node))

def leaf_node_split_and_insert(cursor, key, value):
    old_node = table.pager.get_page(cursor.page_num)
    old_max = get_node_max_key(table.pager, old_node)
    
    new_page_num = table.pager.get_unused_page_num()
    new_node = table.pager.get_page(new_page_num)
    initialize_leaf_node(new_node)
    
    set_node_parent(new_node, node_parent(old_node))
    set_leaf_node_next_leaf(new_node, leaf_node_next_leaf(old_node))
    set_leaf_node_next_leaf(old_node, new_page_num)
    
    # Split cells
    for i in range(LEAF_NODE_MAX_CELLS, -1, -1):
        if i >= LEAF_NODE_LEFT_SPLIT_COUNT:
            destination_node = new_node
        else:
            destination_node = old_node
            
        index_within = i % LEAF_NODE_LEFT_SPLIT_COUNT
        dest_off = leaf_node_cell_offset(index_within)
        
        if i == cursor.cell_num:
            # Insert new value
            struct.pack_into('<I', destination_node, dest_off, key)
            serialize_row(value, destination_node, dest_off + LEAF_NODE_KEY_SIZE)
        elif i > cursor.cell_num:
            src_off = leaf_node_cell_offset(i - 1)
            chunk = old_node[src_off : src_off + LEAF_NODE_CELL_SIZE]
            destination_node[dest_off : dest_off + LEAF_NODE_CELL_SIZE] = chunk
        else:
            src_off = leaf_node_cell_offset(i)
            chunk = old_node[src_off : src_off + LEAF_NODE_CELL_SIZE]
            destination_node[dest_off : dest_off + LEAF_NODE_CELL_SIZE] = chunk

    set_leaf_node_num_cells(old_node, LEAF_NODE_LEFT_SPLIT_COUNT)
    set_leaf_node_num_cells(new_node, LEAF_NODE_RIGHT_SPLIT_COUNT)
    
    if is_node_root(old_node):
        create_new_root(table, new_page_num)
    else:
        parent_page_num = node_parent(old_node)
        new_max = get_node_max_key(table.pager, old_node)
        parent = table.pager.get_page(parent_page_num)
        
        update_internal_node_key(parent, old_max, new_max)
        internal_node_insert(table, parent_page_num, new_page_num)

def leaf_node_insert(cursor, key, value):
    node = table.pager.get_page(cursor.page_num)
    num_cells = leaf_node_num_cells(node)
    
    if num_cells >= LEAF_NODE_MAX_CELLS:
        leaf_node_split_and_insert(cursor, key, value)
        return
        
    if cursor.cell_num < num_cells:
        # Shift cells right
        for i in range(num_cells, cursor.cell_num, -1):
            src_off = leaf_node_cell_offset(i - 1)
            dest_off = leaf_node_cell_offset(i)
            node[dest_off : dest_off + LEAF_NODE_CELL_SIZE] = node[src_off : src_off + LEAF_NODE_CELL_SIZE]
            
    set_leaf_node_num_cells(node, num_cells + 1)
    set_leaf_node_key(node, cursor.cell_num, key)
    serialize_row(value, node, leaf_node_value_offset(cursor.cell_num))

# --- Application Logic ---

def table_start(table):
    cursor = table_find(table, 0)
    node = table.pager.get_page(cursor.page_num)
    num_cells = leaf_node_num_cells(node)
    cursor.end_of_table = (num_cells == 0)
    return cursor

def cursor_advance(cursor):
    node = table.pager.get_page(cursor.page_num)
    cursor.cell_num += 1
    if cursor.cell_num >= leaf_node_num_cells(node):
        next_page = leaf_node_next_leaf(node)
        if next_page == 0:
            cursor.end_of_table = True
        else:
            cursor.page_num = next_page
            cursor.cell_num = 0

def execute_insert(statement, table):
    row = statement.row_to_insert
    key = row.id
    cursor = table_find(table, key)
    node = table.pager.get_page(cursor.page_num)
    num_cells = leaf_node_num_cells(node)
    
    if cursor.cell_num < num_cells:
        key_at_index = leaf_node_key(node, cursor.cell_num)
        if key_at_index == key:
            return ExecuteResult.DUPLICATE_KEY
            
    leaf_node_insert(cursor, key, row)
    return ExecuteResult.SUCCESS

def execute_select(statement, table):
    cursor = table_start(table)
    while not cursor.end_of_table:
        node = table.pager.get_page(cursor.page_num)
        data = node[leaf_node_value_offset(cursor.cell_num) : leaf_node_value_offset(cursor.cell_num) + ROW_SIZE]
        row = deserialize_row(data)
        print(row)
        cursor_advance(cursor)
    return ExecuteResult.SUCCESS

def execute_statement(statement, table):
    if statement.type == StatementType.INSERT:
        return execute_insert(statement, table)
    elif statement.type == StatementType.SELECT:
        return execute_select(statement, table)

# --- CLI and Debugging ---

def print_tree(pager, page_num, indentation_level):
    node = pager.get_page(page_num)
    
    if get_node_type(node) == NodeType.LEAF:
        num_keys = leaf_node_num_cells(node)
        print("  " * indentation_level + f"- leaf (size {num_keys})")
        for i in range(num_keys):
            print("  " * (indentation_level + 1) + f"- {leaf_node_key(node, i)}")
    elif get_node_type(node) == NodeType.INTERNAL:
        num_keys = internal_node_num_keys(node)
        print("  " * indentation_level + f"- internal (size {num_keys})")
        if num_keys > 0:
            for i in range(num_keys):
                child = internal_node_child(node, i)
                print_tree(pager, child, indentation_level + 1)
                print("  " * (indentation_level + 1) + f"- key {internal_node_key(node, i)}")
            child = internal_node_right_child(node)
            print_tree(pager, child, indentation_level + 1)

def print_constants():
    print(f"ROW_SIZE: {ROW_SIZE}")
    print(f"COMMON_NODE_HEADER_SIZE: {COMMON_NODE_HEADER_SIZE}")
    print(f"LEAF_NODE_HEADER_SIZE: {LEAF_NODE_HEADER_SIZE}")
    print(f"LEAF_NODE_CELL_SIZE: {LEAF_NODE_CELL_SIZE}")
    print(f"LEAF_NODE_SPACE_FOR_CELLS: {LEAF_NODE_SPACE_FOR_CELLS}")
    print(f"LEAF_NODE_MAX_CELLS: {LEAF_NODE_MAX_CELLS}")

def do_meta_command(user_input, table):
    if user_input == ".exit":
        table.close()
        sys.exit(0)
    elif user_input == ".btree":
        print("Tree:")
        print_tree(table.pager, 0, 0)
        return MetaCommandResult.SUCCESS
    elif user_input == ".constants":
        print("Constants:")
        print_constants()
        return MetaCommandResult.SUCCESS
    return MetaCommandResult.UNRECOGNIZED_COMMAND

def prepare_statement(user_input, statement):
    if user_input.startswith("insert"):
        statement.type = StatementType.INSERT
        parts = user_input.split()
        if len(parts) != 4: return PrepareResult.SYNTAX_ERROR
        try:
            r_id = int(parts[1])
        except ValueError: return PrepareResult.SYNTAX_ERROR
        if r_id < 0: return PrepareResult.NEGATIVE_ID
        if len(parts[2]) > COLUMN_USERNAME_SIZE: return PrepareResult.STRING_TOO_LONG
        if len(parts[3]) > COLUMN_EMAIL_SIZE: return PrepareResult.STRING_TOO_LONG
        statement.row_to_insert = Row(r_id, parts[2], parts[3])
        return PrepareResult.SUCCESS
    if user_input.startswith("select"):
        statement.type = StatementType.SELECT
        return PrepareResult.SUCCESS
    return PrepareResult.UNRECOGNIZED_STATEMENT

# --- Main ---

if __name__ == "__main__":
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
            result = do_meta_command(user_input, table)
            if result == MetaCommandResult.SUCCESS: continue
            elif result == MetaCommandResult.UNRECOGNIZED_COMMAND:
                print(f"Unrecognized command '{user_input}'")
                continue

        statement = Statement()
        result = prepare_statement(user_input, statement)
        
        if result == PrepareResult.SUCCESS:
            exec_res = execute_statement(statement, table)
            if exec_res == ExecuteResult.SUCCESS: print("Executed.")
            elif exec_res == ExecuteResult.DUPLICATE_KEY: print("Error: Duplicate key.")
            elif exec_res == ExecuteResult.TABLE_FULL: print("Error: Table full.")
        elif result == PrepareResult.SYNTAX_ERROR: print("Syntax error. Could not parse statement.")
        elif result == PrepareResult.NEGATIVE_ID: print("ID must be positive.")
        elif result == PrepareResult.STRING_TOO_LONG: print("String is too long.")
        elif result == PrepareResult.UNRECOGNIZED_STATEMENT: print(f"Unrecognized keyword at start of '{user_input}'.")