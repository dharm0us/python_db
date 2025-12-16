# https://cstack.github.io/db_tutorial/parts/part1.html
import sys

def main():
    # Infinite loop equivalent to while(true)
    while True:
        try:
            # This combines print_prompt() and read_input().
            # input() waits for user input and automatically handles 
            # memory allocation. It also strips the trailing newline.
            user_input = input("db > ")
        except EOFError:
            # Handles Ctrl+D (End of File)
            sys.exit(0)

        # Process the command
        if user_input == ".exit":
            # Equivalent to close_input_buffer() and exit(EXIT_SUCCESS)
            # Python's Garbage Collector handles the cleanup.
            sys.exit(0)
        else:
            print(f"Unrecognized command '{user_input}'.")

if __name__ == "__main__":
    main()