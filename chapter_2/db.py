import sys
from enum import Enum, auto

# --- Enums (Equivalent to typedef enum in C) ---
class MetaCommandResult(Enum):
    SUCCESS = auto()
    UNRECOGNIZED_COMMAND = auto()

class PrepareResult(Enum):
    SUCCESS = auto()
    UNRECOGNIZED_STATEMENT = auto()

class StatementType(Enum):
    INSERT = auto()
    SELECT = auto()

# --- Structures (Equivalent to typedef struct in C) ---
class Statement:
    def __init__(self):
        self.type = None

# --- Meta Command Handling ---
def do_meta_command(user_input):
    if user_input == ".exit":
        sys.exit(0)
    else:
        return MetaCommandResult.UNRECOGNIZED_COMMAND

# --- SQL Compiler (Prepare) ---
def prepare_statement(user_input, statement):
    # C uses strncmp to check the start of the string
    if user_input.startswith("insert"):
        statement.type = StatementType.INSERT
        return PrepareResult.SUCCESS
    
    if user_input.startswith("select"):
        statement.type = StatementType.SELECT
        return PrepareResult.SUCCESS

    return PrepareResult.UNRECOGNIZED_STATEMENT

# --- Virtual Machine (Execute) ---
def execute_statement(statement):
    if statement.type == StatementType.INSERT:
        print("This is where we would do an insert.")
    elif statement.type == StatementType.SELECT:
        print("This is where we would do a select.")

# --- Main Logic ---
def main():
    while True:
        try:
            user_input = input("db > ")
        except EOFError:
            sys.exit(0)

        # Check for Meta-Commands (starting with '.')
        if user_input.startswith("."):
            result = do_meta_command(user_input)
            if result == MetaCommandResult.SUCCESS:
                continue
            elif result == MetaCommandResult.UNRECOGNIZED_COMMAND:
                print(f"Unrecognized command '{user_input}'")
                continue

        # If not a meta-command, treat as an SQL statement
        statement = Statement()
        prepare_result = prepare_statement(user_input, statement)

        if prepare_result == PrepareResult.SUCCESS:
            execute_statement(statement)
            print("Executed.")
        elif prepare_result == PrepareResult.UNRECOGNIZED_STATEMENT:
             print(f"Unrecognized keyword at start of '{user_input}'.")

if __name__ == "__main__":
    main()